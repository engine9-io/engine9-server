require('dotenv').config({ path: `${__dirname}/../../.env` });
const debug = require('debug')('SQSJobScheduler');

debug('Debug settings:', process.env.DEBUG);

const { mkdirp } = require('mkdirp');
const { Consumer } = require('sqs-consumer');
const { Producer } = require('sqs-producer');
const { v7: uuidv7 } = require('uuid');
const { SQSClient } = require('@aws-sdk/client-sqs');
const { createQueue, QueueOrder } = require('simple-in-memory-queue');
const Manager = require('./Manager');
const EchoWorker = require('../workers/EchoWorker');// just to get the environment

/*
  The SQS Job scheduler receives events from SQS,
  and delegates them to the Job Manager to process them.
  Do not get confused with the queues -- there's SQS, which pulls
  and pushes messages from the wire, and then there's *internal* memory queues
  that are how the Manager handles data

*/

function Scheduler(opts) {
  this.opts = opts || {};
}

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

  this.toSchedulerQueue.on.push.subscribe({
    consumer: ({ items }) => {
      items.forEach((item) => {
        item.job_id = item.job_id || item.jobId;
        item.account_id = item.account_id || item.accountId;
        scheduler.handleEvent(item);
      });
    },
  });

  // create simple producer
  this.sqsToScheduler = Producer.create({
    queueUrl: process.env.JOB_OUTBOUND_QUEUE,
    region: process.env.AWS_REGION,
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID,
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    },
  });
  /*
  {"event_type":"job_progress","account_id":"risingtide","job_id":"678ae7fe8003c57c193545bd",
  "progress":"Request acknowledged ... working."}
  {"event_type":"job_complete","account_id":"risingtide","job_list_id":"678ae7fe8003c57c193545ce","job_id":"678ae7fe8003c57c193545cf","output":{"access_token":"ya29.a0ARW5m75acJ1W2NYxR1IqEtlNDgA-jR31zfbbsxPERg063_jgwA2qY3kNvpC_hcu_SgVZv57k2qOAY_PPmk9MF6lO9p_5lM9v6v1wkM6aBEm1YrAX4Xnzd06Cklx4Gsuvm1It-iRQycBV8DUr5IPC5LraVkYvmtYYZYrGkSM9BnYaCgYKAbASARMSFQHGX2Mit6XWOqf2H2mhwjxG_4sCvw0178","expires_in":3599,"scope":"https://www.googleapis.com/auth/adwords","token_type":"Bearer","expiry_date":1737160210903,"metadata":{"resolved_options":{"bot_id":"google_ads_faa"},"runtime":68}}}
  */

  const inboundSQS = Consumer.create({
    queueUrl: process.env.JOB_INBOUND_QUEUE,
    handleMessage: async (message) => {
      let job = message.Body;
      if (typeof job === 'string') job = JSON.parse(job);
      const eventType = job.event_type;

      return this.handleEvent({ eventType, job });
    },
    sqs: new SQSClient({
      region: process.env.AWS_REGION,
      credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
      },
    }),
  });

  inboundSQS.on('error', (err) => { debug('Error:', err.message); });
  inboundSQS.on('processing_error', (err) => { debug('Processing error:', err.message); });

  inboundSQS.start();
  debug(`SQSJobScheduler started polling ${process.env.JOB_INBOUND_QUEUE}`);
};

Scheduler.prototype.handleEvent = async function (event) {
  const { eventType, job = {} } = event;
  const jobId = job?.jobId || job?.job_id || job?.id;
  if (eventType.startsWith('job_') && !jobId) {
    throw new Error('A job event was sent with no job.jobId');
  }
  job.jobId = jobId;
  job.workerPath = job.worker_path || job.bot?.path?.split('.')?.pop();
  job.workerMethod = job.worker_method || job.bot?.method;
  job.accountId = job.account_id || job.accountId;

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
      break;
    }
    case 'job_go': {
      job.options = job.options || {};
      if (typeof job.options === 'string')job.options = JSON.parse(job.options);
      this.toManagerQueue.push({ eventType: 'job_start', job });
      break;
    }
    /* these return from the manager to the queue */
    case 'job_modify':
    case 'job_error':
    case 'job_complete': {
      const id = uuidv7();
      job.job_id = job.job_id || job.jobId;
      job.account_id = job.account_id || job.accountId;
      const queueItem = await this.sqsToScheduler.send([{
        id,
        // groupId: job.accountId || 'group',
        body: JSON.stringify({ event_type: eventType, ...job }),
      }]);

      debug(`Sent queue item ${eventType} for unique id`, id, queueItem);
      break;
    }
    default:
      throw new Error(`Scheduler Error -- could not handle event of type ${event.eventType}`);
  }
  return null;
};

if (require.main === module) {
  const s = new Scheduler();
  s.init();
}

module.exports = Scheduler;
