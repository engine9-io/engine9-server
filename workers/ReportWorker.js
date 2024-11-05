const util = require('util');
// const debug = require('debug')('ReportWorker');
// const debugInfo = require('debug')('info:ReportWorker');

// const JSON5 = require('json5');// Useful for parsing extended JSON

const SQLWorker = require('./SQLWorker');

require('dotenv').config({ path: '.env' });

function Worker(worker) {
  SQLWorker.call(this, worker);
  this.accountId = worker.accountId;
  if (worker.knex) {
    this.knex = worker.knex;
  } else {
    this.auth = {
      ...worker.auth,
    };
  }
}

util.inherits(Worker, SQLWorker);

Worker.prototype.compile = function compile({ report }) {
  if (typeof report !== 'object') throw new Error('compile requires a report object');

  return report;
};

Worker.prototype.compile.metadata = {
  options: {
    query: {},
  },
};

module.exports = Worker;
