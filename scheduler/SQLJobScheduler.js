const util = require('node:util');
const { createQueue, QueueOrder } = require('simple-in-memory-queue');
const debug = require('debug')('SQLJobScheduler');
const Manager = require('./Manager');
const WorkerRunner = require('./WorkerRunner');// just to get the environment
const SQLWorker = require('../workers/SQLWorker');// just to get the environment
const EchoWorker = require('../workers/EchoWorker');// just to get the environment

/*

  The scheduler is responsible for querying the database for new jobs,
  and adding those jobs to the queue, where they'll be picked up the by Runner.
  It is also responsible for listening to responses from the worker manager and
  updating the database appropriately.

*/

function Scheduler(opts) {
  this.opts = opts;
}

Scheduler.prototype.init = async function () {
  const scheduler = this;
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

  const runner = new WorkerRunner();
  const env = runner.getWorkerEnvironment(this.opts);
  this.sqlWorker = new SQLWorker(env);

  this.toSchedulerQueue.on.push.subscribe({
    consumer: ({ items }) => {
      items.forEach((item) => {
        scheduler.handleEvent(item);
      });
    },
  });
};

Scheduler.prototype.poll = async function ({ repeatMilliseconds = 500 }) {
  if (this.is_polling) return;
  this.is_polling = true;
  const { data: jobs } = await this.sqlWorker
    .query(`select id as jobId from job where status='pending' 
        and (start_after is null or start_after<now())
      `);
  if (jobs.length > 0) {
    await Promise.all(jobs.map((job) => this.handleEvent({ eventType: 'job_go', job })));
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
      await this.sqlWorker.query('update job set status=? where id=?', ['kill_sent', jobId]);

      break;
    }
    case 'job_go': {
      const job = (await this.sqlWorker.query('select * from job where id=?', [jobId]))?.data?.[0];
      // map to camel case
      Object.entries({
        jobId: 'id',
        workerPath: 'worker_path',
        workerMethod: 'worker_method',
        accountId: 'account_id',
      }).forEach(([k, v]) => { job[k] = job[v]; delete job[v]; });
      job.options = job.options || {};
      if (typeof job.options === 'string')job.options = JSON.parse(job.options);
      await this.sqlWorker.query('update job set status=? where id=?', ['sent_to_queue', jobId]);
      this.toManagerQueue.push({ eventType: 'job_start', job });

      break;
    }
    /* these return from the manager */
    case 'job_modify':
    {
      const job = (await this.sqlWorker.query('select * from job where id=?', [jobId]))?.data?.[0];

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

      await this.sqlWorker.query(`update job set ${fields.join(',')} where id=?`, values);
      break;
    }
    case 'job_error':
    {
      const job = (await this.sqlWorker.query('select * from job where id=?', [jobId]))?.data?.[0];
      let { error } = event;
      let err = {};
      if (error instanceof Error) {
        err.stack = error.stack;
        err.message = error.toString();
      } else if (typeof error === 'string') {
        // Only allow the first 500 chars of an error
        error = error.slice(0, 500);
        err.stack = new Error(error).stack;
        err.message = error;
      } else if (typeof error === 'object') {
        err = error;
        if (!err.message && err.error) {
          if (!err.level) {
            err = err.error;
          }
        }
        if (!err.message && err.sqlMessage) err.message = err.sqlMessage;
        if (!err.message) err.message = 'Error (see details below)';
        if (!err.stack) err.stack = new Error(util.inspect(error)).stack;

        debug(util.inspect(error, { depth: 5 }));
      } else {
        err.stack = new Error(error).stack;
        err.message = error;
      }
      err.ts = new Date();
      if (error.type) err.type = error.type;
      if (error.level) err.level = error.level;

      const errors = (job.errors || []).push(err);
      let status = 'errored'; // this allows us to retry a few times
      if (errors.length < 4) status = 'pending';

      await this.sqlWorker.query('update job set status=?,errors=? where id=?', [status, errors, jobId]);
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
      await this.sqlWorker.query(`update job set ${fields.join(',')} where id=?`, values);

      break;
    }
    default:
      throw new Error(`Scheduler Error -- could not handle event of type ${event.eventType}`);
  }
  return null;
};

async function start() {
  const scheduler = new Scheduler();
  await scheduler.init();
  scheduler.toManagerQueue.push({ eventType: 'ping' });
  scheduler.poll();
}

if (require.main === module) {
  start();
}
module.exports = Scheduler;
