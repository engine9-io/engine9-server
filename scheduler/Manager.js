const path = require('node:path');
const debug = require('debug')('Manager');
const async = require('async');
const childProcess = require('child_process');

/*
  The job manager listens for inbound events, and manages starting up
  processes to complete those events.

  There is a receiveQueue for jobs and other requests coming to the manager,
  and a toSchedulerQueue for updates to the status of those jobs.

*/

function Manager({ toManagerQueue, toSchedulerQueue }) {
  this.toManagerQueue = toManagerQueue;
  this.toSchedulerQueue = toSchedulerQueue;

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

Manager.prototype.init = async function () {
  return null;
};

Manager.prototype.handleEvent = async function (event) {
  const manager = this;
  // If something has recently completed,
  // OR something has told the scheduler to run, kick off the scheduler
  try {
    // Any SYSTEM errors from the queue will be handled by the done event.
    // JOB errors should be placed on the response channel

    switch (event.eventType) {
      case 'ping':
        manager.toSchedulerQueue.add({ eventType: 'pong' });
        break;
      case 'job_kill':
        manager.killJob(event.job, (_error, success) => {
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
            const job = event.job || event;
            if (!job.accountId) {
              throw new Error('no accountId');
            }

            manager.toSchedulerQueue.add({
              eventType: 'job_error',
              job: {
                accountId: job.accountId,
                jobId: job.jobId,
                error,
              },
            });
          }
        });
        break;
      default:
        manager.forkJob(event.job, (e) => {
          if (e) throw e;
        });
    }
  } catch (error) {
    debug(error);
  }
};

Manager.prototype.getLogPath = function (job) {
  if (!job.jobId) throw new Error(`Could not find jobId in ${JSON.stringify(job)}`);
  const logDir = process.env.ENGINE9_LOG_DIR || '/var/log/engine9';
  return `${logDir + path.sep}jobs${path.sep}${job.accountId}_${job.jobId}.txt`;
};

Manager.prototype.getColor = function () {
  const manager = this;
  const colorList = ['blue', 'green', 'yellow', 'magenta', 'cyan', 'white', 'gray'];

  const color = colorList.filter((c) => !Object.keys(manager.runningWorkers)
    .some((i) => manager.runningWorkers[i].color === c));
  if (color.length > 0) return color[0];
  return 'blue';
};

Manager.prototype.getLogger = function () {
  return debug;
};

Manager.prototype.forkJob = function (_job, callback) {
  const manager = this;
  const job = JSON.parse(JSON.stringify(_job));// local copy for logging, etc

  const missing = ['jobId', 'workerPath', 'workerMethod', 'accountId', 'options'].filter((f) => !job[f]);

  if (missing.length > 0) {
    return callback(new Error(`Job event is missing some required fields:${missing.join()}. Current fields=${Object.keys(job)}`));
  }

  // Kill if necessary
  function killIfRunning(cb) {
    if (manager.runningWorkers[job.jobId]) {
      // Trying to figure out why jobs are getting respawned
      return callback(`Cowardly refusing to re-spawn job ${job.jobId} that's already running.  Kill explicitly`);
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
      eventType: 'job_modify',
      job: {
        accountId: job.accountId,
        jobId: job.jobId,
        progress: 'Request acknowledged ... working.',
      },
    };

    manager.toSchedulerQueue.add(ack);
    debug(`${new Date().toISOString()}\tSent ack for job ${job.jobId} from account ${job.accountId}`);

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

      manager.toSchedulerQueue.add({
        eventType: 'job_error',
        job: {
          accountId: job.accountId,
          jobId: job.jobId,
          error: e.stack || e,
        },
      });
      return callback();
    }

    let prefix = `${job.accountId} ${job.workerPath}.${job.workerMethod}`;

    logger({ type: 'start', message: `Starting ${prefix}` });
    logger({ type: 'options', message: JSON.stringify(job.options || '{}') });

    prefix += ': ';

    const forkParams = [job.workerPath, job.workerMethod,
      `--accountId=${job.accountId}`,
      `_jobId=${job.jobId}`,
      `_jobLog=${manager.getLogPath(job)}`,
    ];
    const runnerPath = `${__dirname}/WorkerRunner.js`;

    debug(`Forking job ${job.jobId} ${prefix} ${runnerPath} ${forkParams.join(' ')} ${Object.keys(job.options).map((k) => `--${k}=${JSON.stringify(job.options[k])}`).join(' ')}`);

    const opts = { silent: true };
    opts.env = { ...process.env };

    // Different debugging options depending on what debug level we have set
    opts.env.DEBUG = job.debug || '';
    const fork = childProcess.fork(
      runnerPath,
      forkParams,
      opts,
    );
    let logToClient = true;
    if (process.env.NODE_DEBUG && process.env.NODE_DEBUG.indexOf('Manager') >= 0) logToClient = true;

    fork.color = color;

    manager.runningWorkers[job.jobId] = fork;

    fork.stderr.on('data', (data) => {
      // convert from buffer
      lastStdErr = data.toString();
      // console.log("stderr:",data.length);

      logger(`${job.jobId}: ${lastStdErr}`);

      if (logToClient) debug((prefix + data.toString()).trim()[fork.color]);
    });

    // messages sent from child messages can be error or progress messages
    fork.on('message', (m) => {
      if (logToClient) debug('Message received:', m);
      if (m.message_type === 'error') {
        if (logToClient) debug('Received error, setting processError');
        processError = m.data;
      } else if (m.message_type === 'checkpoint') {
        if (logToClient) debug('Checkpoint:'[fork.color], m.data);
        manager.toSchedulerQueue.add({
          eventType: 'job_checkpoint',
          job: {
            accountId: job.accountId,
            jobId: job.jobId,
            checkpoint: m.data,
          },
        });
      } else if (m.message_type === 'spawn_dataflow') {
        if (logToClient) debug('Spawn Dataflow:'[fork.color], m.data);
        manager.toSchedulerQueue.add({
          eventType: 'spawn_dataflow',
          job: {
            accountId: job.accountId,
            jobId: job.jobId,
            data: m.data,
          },
        });
      } else if (m.message_type === 'warn') {
        if (logToClient) debug('Warning:'[fork.color], m.data);
        manager.toSchedulerQueue.add({
          eventType: 'job_warn',
          job: {
            accountId: job.accountId,
            jobId: job.jobId,
            warning: m.data,
          },
        });
      } else {
        if (logToClient) debug(new Date().toString(), 'Progress:'[fork.color], m.data);
        debug('Unknown message received from child:', m);
        job.accountId = job.account_id || job.accountId;
        manager.toSchedulerQueue.add({
          eventType: 'job_modify',
          job: {
            accountId: job.accountId,
            jobId: job.jobId,
            progress: m.data,
          },
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
      if (logToClient) debug(`Job ${prefix} ${job.jobId} ended with code ${code}`);
      delete manager.runningWorkers[job.jobId];
      if (code !== 0) {
        // send a blank error, hopefully grab from the fork.on('message') setting
        return jobError();
      }

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

      if (out.job_response_type === 'modify') {
        const { modify } = out;
        debug(`Modifying job ${job.accountId}:${job.jobId}`, job.options, modify);
        if (typeof modify !== 'object') {
          return jobError(`Error: Bad method. Non-object modification request, is:${typeof modify},${JSON.stringify(modify)}`);
        }
        logger({ type: 'modify', message: modify });

        manager.toSchedulerQueue.add({
          eventType: 'job_modify',
          job: { ...modify, jobId: job.jobId, accountId: job.accountId },
        });
      } else {
        debug(`Successfully completed job ${job.accountId}:${job.jobId}`, job.options, out);
        manager.toSchedulerQueue.add({
          eventType: 'job_complete',
          job: {
            accountId: job.accountId,
            jobId: job.jobId,
            output: out,
          },
        });
      }
      return callback();
    });

    fork.stdin.write(JSON.stringify(job.options));
    fork.stdin.end();
    debug(`${new Date().toISOString()}\tForked job ${job.jobId} -- current forks:${Object.keys(manager.runningWorkers)}`);
    return null;
  });
};

Manager.prototype.list = function list(options, callback) {
  const manager = this;

  manager.init({}, (exc) => {
    if (exc) return callback(exc);
    debug('Cleaning out receive queue...');
    const result = {};

    return manager.toSchedulerQueue.list({}, (e, d) => {
      if (e) return callback(e);
      result.toSchedulerQueue = d;
      // eslint-disable-next-line no-shadow
      return manager.toManagerQueue.list({}, (e, d) => {
        if (e) return callback(e);
        result.receiveQueue = d;

        return callback(null, result);
      });
    });
  });
};

Manager.prototype.clear = function clear(options, callback) {
  debug('Clearing out receive queue...');
  const manager = this;

  manager.init({}, (exc) => {
    if (exc) return callback(exc);
    return manager.toSchedulerQueue.clear({}, (e, d) => {
      debug(d);
      return manager.toManagerQueue.clear({}, callback);
    });
  });
};

Manager.prototype.killJob = function killJob(job, callback) {
  const manager = this;

  if (manager.runningWorkers[job.jobId]) {
    debug(`Killing job ${job.jobId} with reason:${job.killReason}`);
    const running = manager.runningWorkers[job.jobId];
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
    delete manager.runningWorkers[job.jobId];
    return callback(null, true);
  }
  debug(`Could not find job ${job.jobId}, not killing`);
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
      options: { foo: 'input', arr: [1, 2, 3], now: new Date() },
    };

    const arr = []; for (let i = 0; i < 1; i += 1) { arr.push(i); }

    return async.eachLimit(arr, 50, (i, cb) => {
      client.forkJob({ ...echo, jobId: `test_${i}`, options: { seconds: parseInt(Math.random() * 3, 10) } }, cb);
    }, (e2) => {
      debug('Completed test_fork');
      if (e2) debug(`There was at least one error:${e2.stack}`);
      callback();
    });
  });
};

module.exports = Manager;
