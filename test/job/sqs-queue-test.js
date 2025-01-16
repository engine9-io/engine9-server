const { Consumer } = require('sqs-consumer');
const { SQSClient } = require('@aws-sdk/client-sqs');
const debug = require('debug');

debug(__dirname);
require('dotenv').config({ path: `${__dirname}/../../.env` });

const app = Consumer.create({
  queueUrl: process.env.JOB_INBOUND_QUEUE,
  handleMessage: async (message) => {
    // do some work with `message`
    debug('Message received:');
    debug(message);
  },
  sqs: new SQSClient({
    region: process.env.AWS_REGION,
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID,
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    },
  }),
});

app.on('error', (err) => {
  debug('Error:', err.message);
});

app.on('processing_error', (err) => {
  debug('Processing error:', err.message);
});

app.start();
