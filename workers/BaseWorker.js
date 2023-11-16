const debug = require('debug')('BaseWorker');

function Worker(worker) {
  if (worker) {
    this.account_id = String(worker.account_id);
    if (worker.file_override_account_id) {
      this.file_override_account_id = worker.file_override_account_id;
    }
    this.log = worker.log;
    this.progress = worker.progress;
    this.warn = worker.warn;
    if (worker.worker_id) this.worker_id = worker.worker_id;
    if (worker.auth) this.auth = worker.auth;
    if (worker.warehouse_worker_id) this.warehouse_worker_id = worker.warehouse_worker_id;
    // share the warehouse worker
    if (worker.warehouse_worker) this.warehouse_worker = worker.warehouse_worker;
    if (worker.checkpoint) this.checkpoint = worker.checkpoint;
    if (worker.status_code) this.status_code = worker.status_code;
    this.parent_worker = worker; // The worker that builds this one out

    if (worker.access_layer_delegate) this.access_layer_delegate = worker.access_layer_delegate;
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

module.exports = Worker;
