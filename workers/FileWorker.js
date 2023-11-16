const util = require('util');
const BaseWorker = require('./BaseWorker');

function Worker(worker) {
  BaseWorker.call(this, worker);
}

util.inherits(Worker, BaseWorker);
Worker.metadata = {
  alias: 'file',
  channel: 'utility',
};

module.exports = Worker;
