const debug = require('debug')('Worker');
const async = require('async')('Worker');
const childProcess = require('child_process');

function Manager() {
  const runningWorkers = {};
  this.runningWorkers = runningWorkers;

  // When the parent exits, it should be non-terminal (SIGHUP)
  // If a command is given, it should be SIGTERM (Stop now, don't restart)
  function end() {
    Object.entries(runningWorkers).forEach(([id, worker]) => {
      debug(`Killing ${id}`);
      worker.kill('SIGHUP');
    });
  }

  process.on('SIGTERM', end);
  process.on('exit', end);
}

Manager.prototype.listen = function listen(options, callback) {
  const manager = this;
  manager.clear({}, (e) => {
    if (e) return callback(e);
    debug('Cleared the receive queue');

    manager.receiveQueue.subscribe({
      handler(event, channelComplete) {
        debug('Subscribed to the receive queue');

        // If something has recently completed,
        // OR something has told the scheduler to run, kick off the scheduler
        try {
        // Any SYSTEM errors from the queue will be handled by the done event.
        // JOB errors should be placed on the response channel

          switch (event.event_type) {
            case 'heartbeat':
              manager.processHeartbeat(event, channelComplete);
              break;
            case 'job_kill':
              manager.killJob(event, (_error, success) => {
                if (success) {
                // If it was successfully killed, no other message is necessary
                // Job NOT successfully killed
                  debug(_error);
                } else {
                // Not successfully killed, meaning it wasn't running.
                // Should still track why it was intended to be killed

                  const error = (event.killReason === 'timeout')
                    ? { message: 'Killed via timeout' }
                    : { level: 'CRITICAL', message: "Killed by user, wasn't running" };

                  if (_error && _error === 'No running job') {
                    error.message += ' (Process had died)';
                  }

                  manager.sendQueue.add({
                    event_type: 'job_error',
                    account_id: event.account_id,
                    job_id: event.job_id,
                    error,
                  });
                }

                channelComplete();
              });
              break;
            default:
            // process it, but flag the channel as complete so we
            // can keep processing other jobs if necessary
              manager.forkJob(event, () => {});
              channelComplete();
          }
        } catch (error) {
          debug(error.stack);
        }
      },
    }, (error) => {
      if (error) return callback(error);
      debug('Listening');
      return null;
    });
    return null;
  });
};

Manager.prototype.forkWorker = function forkJob(_job, callback) {
  const job = JSON.parse(JSON.stringify(_job));// local copy for logging, etc
  const manager = this;

  const missing = ['workerInstance', 'accountId', 'options'].filter((f) => !job[f]);

  if (missing.length > 0) {
    return callback(`Job is missing ${missing.join()}`);
  }

  // Kill if necessary
  function killIfRunning(cb) {
    if (manager.runningJobs[job.job_id]) {
      // Trying to figure out why jobs are getting respawned
      return callback(`Cowardly refusing to re-spawn job ${job.job_id} that's already running.  Kill explicitly`);
      // manager.killJob(job, cb);
    }
    return cb();
  }

  return killIfRunning((exc) => {
    if (exc) return callback(exc);

    // First, store any auth data locally, and delete, so that we don't accidentally log it
    delete job.auth;

    // Acknowledge that we're trying to process the job
    const ack = {
      event_type: 'job_progress',
      accountId: job.accountId,
      job_id: job.job_id,
      progress: 'Request acknowledged ... working.',
    };

    manager.sendQueue.add(ack);
    debug(`${new Date().toISOString()}\tSent ack for job ${job.job_id} from account ${job.accountId}`);

    const logger = manager.getLogger(job);

    const color = manager.getColor();

    let processError = null;
    let lastStdErr = null;
    // Flag the job as an error
    function jobError(_e) {
      let e = _e;
      if (!e) e = processError;
      if (!e) e = lastStdErr;
      if (!e) e = 'Unspecified error';

      try {
        e = JSON.parse(e.trim());
      } catch (exc2) {
        // do nothing
      }

      manager.sendQueue.add({
        event_type: 'job_error',
        accountId: job.accountId,
        job_id: job.job_id,
        error: e.stack || e,
      });
      return callback();
    }

    let prefix = `${job.accountId} ${job.workerInstance.path}.${job.workerInstance.method}`;

    logger({ type: 'start', message: `Starting ${prefix}` });
    logger({ type: 'options', message: JSON.stringify(job.options || '{}') });

    prefix += ': ';

    const forkParams = ['run', job.workerInstance.path, job.workerInstance.method,
      `--accountId=${job.accountId}`,
      `--workerId=${job.assignee.workerId}`,
      `_job_id=${job.job_id}`,
      `_job_log=${manager.getLogPath(job)}`,
    ];

    debug(`Forking ${job.job_id} ${prefix} bin/frakture.js ${forkParams.join(' ')} ${Object.keys(job.options).map((k) => `--${k}=${JSON.stringify(job.options[k])}`).join(' ')}`);

    const opts = { silent: true };
    opts.env = { ...process.env };

    // Different debugging options depending on what debug level we have set
    opts.env.DEBUG = job.debug || '';
    const fork = childProcess.fork(
      'bin/frakture.js',
      forkParams,
      opts,
    );

    let logToClient = false;
    if (process.env.NODE_DEBUG && process.env.NODE_DEBUG.indexOf('Manager') >= 0) logToClient = true;

    fork.color = color;

    manager.runningJobs[job.job_id] = fork;

    fork.stderr.on('data', (data) => {
      // convert from buffer
      lastStdErr = data.toString();
      // console.log("stderr:",data.length);

      logger(`${job.job_id}: ${lastStdErr}`);

      if (logToClient) debug((prefix + data.toString()).trim()[fork.color]);
    });

    // messages sent from child messages can be error or progress messages
    fork.on('message', (m) => {
      if (logToClient) debug('Message received:', m);
      if (m.frakture_type === 'error') {
        if (logToClient) debug('Received error, setting processError');
        processError = m.data;
      } else if (m.frakture_type === 'checkpoint') {
        if (logToClient) debug('Checkpoint:'[fork.color], m.data);
        manager.sendQueue.add({
          event_type: 'job_checkpoint',
          accountId: job.accountId,
          job_id: job.job_id,
          checkpoint: m.data,
        });
      } else if (m.frakture_type === 'job_signal') {
        if (logToClient) debug('Signal Job:'[fork.color], m.data);
        const { data } = m;
        if (job.accountId !== 'system') {
          debug('Can only be carried out by system jobs');
          return;
        }
        const event = { ...data, event_type: 'job_modify' };
        debug('job signal:', event);
        manager.sendQueue.add(event);
      } else if (m.frakture_type === 'spawn_dataflow') {
        if (logToClient) debug('Spawn Dataflow:'[fork.color], m.data);
        manager.sendQueue.add({
          event_type: 'spawn_dataflow',
          accountId: job.accountId,
          job_id: job.job_id,
          data: m.data,
        });
      } else if (m.frakture_type === 'warn') {
        if (logToClient) debug('Warning:'[fork.color], m.data);
        manager.sendQueue.add({
          event_type: 'job_warn',
          accountId: job.accountId,
          job_id: job.job_id,
          warning: m.data,
        });
      } else {
        if (logToClient) debug(new Date().toString(), 'Progress:'[fork.color], m.data);
        manager.sendQueue.add({
          event_type: 'job_progress',
          accountId: job.accountId,
          job_id: job.job_id,
          progress: m.data,
        });
      }
    });

    let output = '';
    fork.stdout.on('data', (data) => {
      const s = data.toString();
      // console.log("stdout received:",data.length);
      if (logToClient) debug((prefix + s).trim()[fork.color]);
      output += s;
      logger(`Stdout coming from client:${s.length} bytes: ${s}`);
    });

    fork.on('close', (code) => {
      if (logToClient) debug(`Job ${prefix} ${job.job_id} ended with code ${code}`);
      delete manager.runningJobs[job.job_id];
      if (code !== 0) {
        // send a blank error, hopefully grab from the fork.on('message') setting
        return jobError();
      }

      debug(`Completed job ${job.job_id} with node version ${process.version}`);

      // Try to parse the output that we have -- could be success or modify.
      // Error SHOULD be handled by invalid error codes
      let out = null;
      try {
        out = JSON.parse(output);
      } catch (e) {
        if (logToClient) debug('Bad output, length:', output.length);
        if (output.length >= 8192) {
          logger('Could not parse output, error=', e);
          return jobError(`Perhaps too much data in the output -- please filter output, output length=${output.length}`);
        }
        if (logToClient) debug('Could not parse output starting with ', output.slice(0, 50), 'ending with', output.slice(-50));
        debug(e);
        // String output not allowed -- at least until we fix some things
        return jobError(`Invalid, non-JSON output.  Look for 'console.log', should be 'console.err':Content starts with ${output.slice(0, 30)}, ${output}`);
        // out=output;
      }

      if (out.frakture_response_type === 'modify') {
        const { modify } = out;
        debug(`Modifying job ${job.accountId}:${job.job_id}`, job.options, modify);
        if (typeof modify !== 'object') {
          return jobError(`Error: Bad method. Non-object modification request, is:${typeof modify},${JSON.stringify(modify)}`);
        }
        logger({ type: 'modify', message: modify });
        modify.event_type = 'job_modify';
        modify.accountId = job.accountId;
        modify.job_id = job.job_id;
        manager.sendQueue.add(modify);
      } else {
        debug(`Successfully completed job ${job.accountId}:${job.job_id}`, job.options, out);
        manager.sendQueue.add({
          event_type: 'job_complete',
          accountId: job.accountId,
          job_list_id: job.job_list_id,
          job_id: job.job_id,
          output: out,
        });
      }

      return callback();
    });

    fork.stdin.write(JSON.stringify(job.options));
    fork.stdin.end();
    debug(`${new Date().toISOString()}\tForked job ${job.job_id} -- current forks:${Object.keys(manager.runningJobs)}`);
    return null;
  });
};

Manager.prototype.list = function list(options, callback) {
  const manager = this;

  manager.init({}, (exc) => {
    if (exc) return callback(exc);
    debug('Cleaning out receive queue...');
    const r = {};

    return manager.sendQueue.list({}, (e, d) => {
      if (e) return callback(e);
      r.sendQueue = d;
      // eslint-disable-next-line no-shadow
      return manager.receiveQueue.list({}, (e, d) => {
        if (e) return callback(e);
        r.receiveQueue = d;

        return callback(null, r);
      });
    });
  });
};

Manager.prototype.clear = function clear(options, callback) {
  debug('Clearing out receive queue...');
  const manager = this;

  manager.init({}, (exc) => {
    if (exc) return callback(exc);
    return manager.sendQueue.clear({}, (e, d) => {
      debug(d);
      return manager.receiveQueue.clear({}, callback);
    });
  });
};

Manager.prototype.killJob = function killJob(job, callback) {
  const manager = this;

  if (manager.runningJobs[job.job_id]) {
    debug(`Killing job ${job.job_id} with reason:${job.killReason}`);
    const running = manager.runningJobs[job.job_id];
    if (job.killReason === 'timeout') {
      // Scheduler decided it was time to stop it.
      running.kill('SIGINT');
    } else if (job.killReason === 'user_request') {
      // Definitely a user wants this to stop.
      running.kill('SIGTSTP');
    } else {
      // kill with a quit -- we're not sure why it's killed, but give it another shot
      running.kill('SIGQUIT');
    }
    delete manager.runningJobs[job.job_id];
    return callback(null, true);
  }
  debug(`Could not find job ${job.job_id}, not killing`);
  return callback('No running job');
};

Manager.prototype.forkTest = function forkTest(job, callback) {
  const client = this;

  client.init({}, (e) => {
    if (e) return callback(e);
    debug('Testing job forking');

    const echo = {
      accountId: 'dev',
      workerInstance: { path: '../workers/EchoWorker.js', method: 'echo' },
      assignee: { type: 'workerInstance', workerId: '' },
      job_list_id: 'abc123',
      options: { foo: 'input', arr: [1, 2, 3], now: new Date() },
    };

    const arr = []; for (let i = 0; i < 1; i += 1) { arr.push(i); }

    return async.eachLimit(arr, 50, (i, cb) => {
      client.forkJob({ ...echo, job_id: `test_${i}`, options: { seconds: parseInt(Math.random() * 3, 10) } }, cb);
    }, (e2) => {
      debug('Completed test_fork');
      if (e2) debug(`There was at least one error:${e2.stack}`);
      callback();
    });
  });
};

const managerInstance = new Manager();
managerInstance.listen();
