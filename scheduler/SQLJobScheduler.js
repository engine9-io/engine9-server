const util = require('node:util');
const { createQueue, QueueOrder } = require('simple-in-memory-queue');
const debug = require('debug')('SQLJobScheduler');
const debugJob = require('debug')('job:SQLJobScheduler');
const debugPoll = require('debug')('poll:SQLJobScheduler');
const debugError = require('debug')('error:SQLJobScheduler');
const dayjs = require('dayjs');
const { mkdirp } = require('mkdirp');
const Manager = require('./Manager');
const WorkerRunner = require('./WorkerRunner');// just to get the environment
const SQLWorker = require('../workers/SQLWorker');// just to get the environment
const EchoWorker = require('../workers/EchoWorker');// just to get the environment
const { relativeDate } = require('../utilities');

/*

  The scheduler is responsible for querying the database for new jobs,
  and adding those jobs to the queue, where they'll be picked up the by Runner.
  It is also responsible for listening to responses from the worker manager and
  updating the database appropriately.

*/

function Scheduler(opts) {
  this.opts = opts || {};
}
Scheduler.prototype.initSQLWorker = function () {
  if (this.sqlWorker) return this.sqlWorker;
  try {
    const runner = new WorkerRunner();
    const accountId = process.env.SCHEDULER_JOB_ACCOUNT_ID || this.opts.accountId || 'engine9';
    if (!accountId) throw new Error('No env.SCHEDULER_JOB_ACCOUNT_ID, or opts.accountId ');
    const env = runner.getWorkerEnvironment({ accountId });
    if (!env) throw new Error(`Could not find configuration for account ${accountId}`);
    this.sqlWorker = new SQLWorker(env);
    return this.sqlWorker;
  } catch (e) {
    debugError(e);
    throw new Error('Could not set up the Scheduler environment and database connection');
  }
};

Scheduler.prototype.init = async function () {
  const scheduler = this;

  try {
    const logDir = process.env.ENGINE9_LOG_DIR || '/var/log/engine9';
    await mkdirp(logDir);
  } catch (err) {
    if (err && err.code !== 'EEXIST') throw err;
  }
  // Create and standardize the queue methods with add/list/clear

  scheduler.toSchedulerQueue = createQueue({ order: QueueOrder.FIRST_IN_FIRST_OUT });
  scheduler.toSchedulerQueue.add = (a) => scheduler.toSchedulerQueue.push(a);
  scheduler.toSchedulerQueue.list = () => scheduler.toSchedulerQueue
    .peek(scheduler.toSchedulerQueue.length);
  scheduler.toSchedulerQueue.clear = () => scheduler.toSchedulerQueue
    .pop(scheduler.toSchedulerQueue.length);

  scheduler.toManagerQueue = createQueue({ order: QueueOrder.FIRST_IN_FIRST_OUT });
  scheduler.toManagerQueue.add = (a) => scheduler.toManagerQueue.push(a);
  scheduler.toManagerQueue.list = () => scheduler.toManagerQueue
    .peek(scheduler.toManagerQueue.length);
  scheduler.toManagerQueue.clear = () => scheduler.toManagerQueue
    .pop(scheduler.toManagerQueue.length);

  this.manager = new Manager({
    toManagerQueue: scheduler.toManagerQueue,
    toSchedulerQueue: scheduler.toSchedulerQueue,
  });

  scheduler.toManagerQueue.on.push.subscribe({
    consumer: ({ items }) => {
      items.forEach((item) => {
        this.manager.handleEvent(item);
      });
    },
  });

  this.initSQLWorker();

  this.toSchedulerQueue.on.push.subscribe({
    consumer: ({ items }) => {
      items.forEach((item) => {
        scheduler.handleEvent(item);
      });
    },
  });
};

Scheduler.prototype.addJob = async function (data) {
  return (await this.sqlWorker.insertOne({ table: 'job', data }))?.data?.[0];
};

Scheduler.prototype.poll = async function ({ repeatMilliseconds = 2000 } = {}) {
  if (this.is_polling) {
    debugPoll('Already polling, returning..');
    return;
  }
  this.is_polling = true;
  const sql = 'select id as jobId,start_after,status from job where status=\'pending\' and (start_after is null or start_after<now())';
  debugPoll(sql);
  const { data: jobs } = await this.sqlWorker.query(sql);
  if (jobs.length > 0) {
    debugPoll(`Found ${jobs.length} jobs, including ${jobs.slice(0, 5).map((d) => d.jobId)}`);
    await Promise.all(jobs.map((job) => {
      debugJob(`Found job ${job.jobId} with status ${job.status} starting after ${job.start_after}`);
      return this.handleEvent({ eventType: 'job_go', job });
    }));
  }
  this.is_polling = false;
  if (repeatMilliseconds) setTimeout(() => this.poll(), repeatMilliseconds);
};

Scheduler.prototype.handleEvent = async function (event) {
  const { eventType } = event;
  const jobId = event?.job?.jobId;
  if (eventType.startsWith('job_') && !jobId) {
    throw new Error('A job event was sent with no job.jobId');
  }

  switch (eventType) {
    case 'ping':
    case 'pong': {
      debug('Scheduler logging ping-pong:', event);

      event.ping_responded = new Date();
      break;
    }
    case 'echo': {
      const echoWorker = new EchoWorker({});
      await echoWorker.echo(event);
      break;
    }
    /* these go to the manager */
    case 'job_kill': {
      this.toManagerQueue.push(event);
      debugJob(`Updating job ${jobId} with status kill_sent`);
      await this.sqlWorker.query({ sql: 'update job set status=? where id=?', values: ['kill_sent', jobId] });

      break;
    }
    case 'job_go': {
      const job = (await this.sqlWorker.query({ sql: 'select * from job where id=?', values: [jobId] }))?.data?.[0];
      // map to camel case
      Object.entries({
        jobId: 'id',
        workerPath: 'worker_path',
        workerMethod: 'worker_method',
        accountId: 'account_id',
      }).forEach(([k, v]) => { job[k] = job[v]; delete job[v]; });
      job.options = job.options || {};
      if (typeof job.options === 'string')job.options = JSON.parse(job.options);
      debugJob(`Updating job ${jobId} set status=sent_to_queue`);
      await this.sqlWorker.query({ sql: 'update job set status=? where id=?', values: ['sent_to_queue', jobId] });
      this.toManagerQueue.push({ eventType: 'job_start', job });

      break;
    }
    /* these return from the manager */
    case 'job_modify':
    {
      const job = (await this.sqlWorker.query({ sql: 'select * from job where id=?', values: [jobId] }))?.data?.[0];

      const fields = []; const values = [];

      if (event.job?.status) {
        fields.push('status=?');
        values.push(event.job?.status);
      } else if (job.status === 'sent_to_queue') {
        // if not specified, and in sent_to_queue, then set status=started and started_at
        fields.push('status=?');
        values.push('started');
        fields.push('started_at=?');
        values.push(new Date());
      }

      if (event.job?.progress) {
        let progress = event.job?.progress || 'Running ..';
        if (typeof progress === 'string') {
          if (progress.indexOf('{') !== 0 && progress.indexOf('[') !== 0) {
            progress = { message: progress };
          } else {
            progress = JSON.parse(progress);
          }
        }
        fields.push('progress=?');
        values.push(JSON.stringify(progress));
      }

      values.push(jobId);
      debugJob(`job_modify: Updating job ${jobId} with ${fields.map((f, i) => f.slice(0, -1) + values[i])}`);
      await this.sqlWorker.query({ sql: `update job set ${fields.join(',')} where id=?`, values });
      break;
    }
    case 'job_error':
    {
      const job = (await this.sqlWorker.query({ sql: 'select * from job where id=?', values: [jobId] }))?.data?.[0];

      const newJob = event.job || event;
      let { error } = newJob;
      if (!error) {
        debugJob({ event });
        throw new Error('No error property in job_error event');
      }
      let err = {};
      if (error instanceof Error) {
        err.stack = error.stack;
        [err.message] = error.toString().split('\n');
      } else if (typeof error === 'string') {
        // Only allow the first 500 chars of an error
        error = error.slice(0, 500);
        err.stack = error;
        [err.message] = String(error).split('\n');
      } else if (typeof error === 'object') {
        err = error;
        if (!err.message && err.error) {
          if (!err.level) {
            err = err.error;
          }
        }
        if (!err.message && err.sqlMessage) err.message = err.sqlMessage;
        if (!err.message) err.message = 'Error (see details below)';
        // if (!err.stack) err.stack = new Error(util.inspect(error)).stack;

        debug(util.inspect(error, { depth: 5 }));
      } else {
        err.stack = new Error(error).stack;
        err.message = error;
      }
      err.ts = new Date();
      if (error.type) err.type = error.type;
      if (error.level) err.level = error.level;

      let errors = (job.errors || []);
      if (typeof errors === 'string') {
        try {
          errors = JSON.parse(errors);
        } catch (e) {
          errors = [errors];
        }
      }
      if (!Array.isArray(errors)) errors = [errors];
      errors.push(err);

      let startAfter = null;

      let status = 'error'; // this allows us to retry a few times, if it's not a critical or authentication error
      if (errors.length < 4) {
        if (err.level && ['critical', 'authentication'].find((d) => d === err.level.toLowerCase())) {
          // critical or auth errors should not be retried
        } else {
          // This calculates how long to wait before another trial
          // Start with immediate, then fall off 20 second per error
          startAfter = relativeDate(`+${(errors.length - 1) * 20}s`);
          status = 'pending';
        }
      }
      debugJob(`After error #${errors.length}, updating job ${jobId} to status ${status}, starting after ${startAfter ? dayjs(startAfter).format('HH:mm:ss') : 'now'},error=`, String(err.message || err).split('\n')[0]);
      await this.sqlWorker.query({ sql: 'update job set status=?,errors=?,progress=?,start_after=? where id=?', values: [status, JSON.stringify(errors, null, 4), null, startAfter, jobId] });
      break;
    }
    case 'job_complete': {
      const fields = [
        'status=?',
      ];
      const values = ['complete'];

      let output = event.job?.output;
      if (output) {
        if (typeof output === 'string') {
          if (output.indexOf('{') !== 0 && output.indexOf('[') !== 0) {
            output = { output };
          } else {
            output = JSON.parse(output);
          }
        } else if (typeof output === 'object') {
          // it's an object
        } else { // integers, etc
          output = { output };
        }
      } else {
        output = {};
      }
      fields.push('output=?');
      values.push(JSON.stringify(output));

      values.push(jobId);
      debugJob(`job_complete: Updating job ${jobId} with ${fields.map((f, i) => f.slice(0, -1) + values[i])}`);
      await this.sqlWorker.query({ sql: `update job set ${fields.join(',')} where id=?`, values });

      break;
    }
    default:
      throw new Error(`Scheduler Error -- could not handle event of type ${event.eventType}`);
  }
  return null;
};

module.exports = Scheduler;
