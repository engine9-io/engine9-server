/* eslint-disable camelcase */
const util = require('node:util');

const BaseWorker = require('./BaseWorker');

function Worker(worker) {
  BaseWorker.call(this, worker);
}

util.inherits(Worker, BaseWorker);

module.exports = Worker;
