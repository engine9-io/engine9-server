const util = require('util');
const { pipeline } = require('node:stream/promises');

const through2 = require('through2');
// const fs = require('fs');
const debug = require('debug')('PersonWorker');
const SQLWorker = require('./SQLWorker');

const ExtensionBaseWorker = require('./ExtensionBaseWorker');

function Worker(worker) {
  ExtensionBaseWorker.call(this, worker);
}

util.inherits(Worker, ExtensionBaseWorker);

Worker.prototype.getPeopleStream = async function () {
  const sqlWorker = new SQLWorker(this);
  const emailExtension = this.compileExtension({ extension_path: 'core_extensions/person_email' });
  const stream = await sqlWorker.stream({ sql: 'select * from person' });

  const { filename, stream: fileStream } = this.getFileWriterStream();
  await pipeline(
    stream,
    emailExtension,
    // this.getJSONStringifyStream().stream,
    through2.obj((o, enc, cb) => {
      debug('Through2:', o);
      cb(null, `${JSON.stringify(o)}\n`);
    }),
    fileStream,
  );

  return { filename };
};

Worker.prototype.getPeopleStream.metadata = {};

module.exports = Worker;
