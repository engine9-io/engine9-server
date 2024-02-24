const debug = require('debug')('BaseWorker');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const util = require('node:util');
const { mkdirp } = require('mkdirp');

const { Transform } = require('node:stream');

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
        cb(false, `${JSON.stringify(d)}\n`);
      },
    }),
  };
};

Worker.prototype.getTempDir = async function () {
  const worker = this;
  const accountPart = worker.account_id || 'unknown';
  const dir = [os.tmpdir(), accountPart, new Date().toISOString().substring(0, 10)].join(path.sep);

  try {
    await mkdirp(dir);
  } catch (err) {
    if (err && err.code !== 'EEXIST') throw err;
  }

  return dir;
};

Worker.prototype.getFileWriterStream = async function (options = {}) {
  const postfix = options.postfix || 'jsonl';
  const tempDir = await this.getTempDir();
  const filename = `${tempDir}${path.sep}${this.accountId}_${new Date().getTime()}.${postfix}`;
  const stream = fs.createWriteStream(filename);
  debug('Writing to file ', filename);

  return { filename, stream };
};

Worker.prototype.getFilename = function (options) {
  const worker = this;
  let f = options.filename;

  if (!f) throw new Error('No filename specified');
  if (typeof f !== 'string') throw new Error(`filename is a ${typeof f}`);

  let hasTilda = false;

  let match = /^[.a-z/_0-9-(),& ]+$/i;
  if (path.sep === '\\') {
    // different windows filter, allow tildas
    match = /^[:~.,a-z\\/_0-9-()' ]+$/i;
  } else if (f.indexOf('~') === 0) {
    if (!worker.accountId || !String(worker.accountId).match(/^[0-9a-z_]*$/)) throw new Error('worker.accountId is invalid');
    hasTilda = true;
    f = f.slice(1);
  }

  if (hasTilda) {
    if (f.indexOf('/') !== 0) f = `/${f}`;
    const prefix = `${process.env.ENGINE9_HOME.replace('/server', '')}/accounts/`;
    f = prefix + worker.accountId + f;
  }

  if (!f) throw new Error('Invalid, empty filename');
  if (!f.match(match)) throw new Error(`Invalid filename:"${options.filename}", does not match ${util.inspect(match)}`);
  if (f.indexOf('..') >= 0) throw new Error(`Invalid filename:"${options.filename}", double dots not allowed`);

  return f;
};

Worker.prototype.getFilename.metadata = {
  options: { filename: { required: true } },
};

module.exports = Worker;
