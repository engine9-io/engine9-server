const util = require('node:util');

const { pipeline } = require('node:stream/promises');
const fs = require('node:fs');
const { Transform } = require('node:stream');
const { stringify } = require('csv');
const { getTempFilename } = require('@engine9/packet-tools');
const PersonWorker = require('./PersonWorker');
const FileWorker = require('./FileWorker');

function Worker(worker) {
  PersonWorker.call(this, worker);
}

util.inherits(Worker, PersonWorker);

Worker.prototype.load = async function (options) {
  const worker = this;
  const { pluginId } = options;
  if (!pluginId) throw new Error('load requires a pluginId');
  const filename = await getTempFilename({ postfix: '.csv' });
  const fileWorker = new FileWorker(this);
  const batcher = this.getBatchTransform({ batchSize: 300 }).transform;
  await pipeline(
    (await fileWorker.fileToObjectStream(options)).stream,
    batcher,
    new Transform({
      objectMode: true,
      async transform(batch, encoding, cb) {
        await worker.appendInputId({ pluginId, batch });
        await worker.appendEntryTypeId({ batch });
        await worker.appendSourceCodeId({ batch });
        await worker.appendPersonId({ batch });
        await worker.appendEntryId({ pluginId, batch });
        batch.forEach((x) => this.push(x));
        cb();
      },
    }),
    stringify({ header: true }),
    fs.createWriteStream(filename),
  );
  return filename;
};

Worker.prototype.load.metadata = {
  options: {
    filename: {},
  },
};

module.exports = Worker;
