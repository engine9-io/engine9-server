const debug = require('debug')('BaseWorker');
const fs = require('fs');
const { Transform } = require('stream');

function Worker(worker) {
  if (worker) {
    this.accountId = String(worker.accountId);
    this.log = worker.log;
    this.progress = worker.progress;
    this.warn = worker.warn;
    if (worker.worker_id) this.worker_id = worker.worker_id;
    if (worker.auth) this.auth = worker.auth;
    if (worker.checkpoint) this.checkpoint = worker.checkpoint;
    if (worker.status_code) this.status_code = worker.status_code;
  }

  this.metadata = this.constructor.metadata || {};
  // progress should be standard, but depending on how a Worker was called it may not be there.
  // Make sure it's there, and we'll override in the constructor
  if (!this.progress) {
    this.progress = function progress(s) {
      debug('Warning, progress method not available from the source worker:', s);
    };
  }
}

Worker.prototype.getJSONStringifyStream = function () {
  return {
    stream: new Transform({
      objectMode: true,
      transform(d, encoding, cb) {
        debug(d);
        cb(false, `${JSON.stringify(d)}/n`);
      },
    }),
  };
};

Worker.prototype.getFileWriterStream = function () {
  const filename = `${this.accountId}_${new Date().getTime()}.json`;
  const stream = fs.createWriteStream(filename);

  return { filename, stream };
};
module.exports = Worker;
