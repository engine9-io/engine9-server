/* eslint-disable camelcase */

const util = require('node:util');
const { pipeline } = require('node:stream/promises');
const { Transform } = require('node:stream');
const fs = require('node:fs');

const fsp = fs.promises;
const path = require('node:path');

const zlib = require('node:zlib');
const csv = require('csv');
const { mkdirp } = require('mkdirp');
const debug = require('debug')('InputWorker');
const { uuidRegex, getTempFilename } = require('@engine9/packet-tools');
// const SQLWorker = require('./SQLWorker');
const SQLiteWorker = require('./sql/SQLiteWorker');
const PluginBaseWorker = require('./PluginBaseWorker');
const FileWorker = require('./FileWorker');
const PersonWorker = require('./PersonWorker');

function Worker(worker) {
  PluginBaseWorker.call(this, worker);
}

util.inherits(Worker, PluginBaseWorker);

/*
  An input storage db gets or creates an input storage DB file,
  currently a SQLite database with a timestamp
*/
const store = process.env.ENGINE9_STORED_INPUT_PATH;
if (!store) throw new Error('No ENGINE9_STORED_INPUT_PATH configured');
Worker.prototype.getTimelineInputSQLiteDB = async function ({
  sqliteFile,
  inputId,
  includeEmailDomain = false,
}) {
  if (!sqliteFile && !inputId) throw new Error('getTimelineInputSQLiteDB requires a sqliteFile or inputId');
  let dbpath = sqliteFile;
  this.timelineSQLiteDBCache = this.timelineSQLiteDBCache || {};
  if (!dbpath) {
    const dir = [store, this.accountId, inputId.slice(0, 4), inputId].join(path.sep);
    await (mkdirp(dir));
    dbpath = `${dir + path.sep}input.db`;
  }

  let output = this.timelineSQLiteDBCache[dbpath];
  if (output) return output;
  const exists = await fsp.stat(dbpath).then(() => true).catch(() => false);
  const sqliteWorker = new SQLiteWorker({ sqliteFile: dbpath, accountId: this.accountId });
  await sqliteWorker.ensureTimelineSchema({ includeEmailDomain });

  if (exists) {
    output = { sqliteFile: dbpath, sqliteWorker };
    this.timelineSQLiteDBCache[dbpath] = output;
    return output;
  }

  output = { sqliteFile: dbpath, sqliteWorker };
  this.timelineSQLiteDBCache[dbpath] = output;
  return output;
};

Worker.prototype.destroy = async function () {
  Object.entries(this.timelineSQLiteDBCache || {}).forEach(([, { sqliteWorker }]) => {
    sqliteWorker.destroy();
  });
};

/*
  Add ids to an input file -- presumed to already be split by inputs
*/
Worker.prototype.id = async function (options) {
  const worker = this;

  const {
    inputId, defaultEntryType, filename,
  } = options;
  if (!inputId) throw new Error('id requires an inputId');
  const processId = `.${new Date().getTime()}.processing`;

  const fileWorker = new FileWorker(this);
  const personWorker = new PersonWorker(this);

  const idStream = csv.stringify({ header: true });
  let outputFile;
  if (filename.indexOf('/') === 0) {
    outputFile = `${filename}.with_ids.csv.gz`;
  } else {
    outputFile = await getTempFilename({ postfix: '.with_ids.csv.gz' });
  }

  const idFileStream = fs.createWriteStream(`${outputFile}${processId}`);

  idStream
    .pipe(zlib.createGzip())
    .pipe(idFileStream);

  /*
  //this is for a separate stream of detail data, but that may just be extraneous
  const detailStream = csv.stringify({ header: true });
  const detailFileStream = fs.createWriteStream(`${filename}.details.csv.gz${processId}`);
  detailStream.pipe(zlib.createGzip()).pipe(detailFileStream);
  */

  const batcher = this.getBatchTransform({ batchSize: 500 }).transform;
  let records = 0;
  let batches = 0;
  await pipeline(
    (await fileWorker.fileToObjectStream({ filename })).stream,
    batcher,
    new Transform({
      objectMode: true,
      async transform(batch, encoding, cb) {
        // eslint-disable-next-line no-underscore-dangle
        if (batch[0]?._is_placeholder) {
          return cb(null);
        }
        const initialItem = { ...batch[0] };

        batches += 1;
        records += batch.length;
        if (batches % 10 === 0) debug(`Processed ${batches} batches, ${records} records`);
        worker.markPerformance('start-entry-type-id');
        await worker.appendEntryTypeId({ batch, defaultEntryType });
        worker.markPerformance('start-source-code-id');
        await worker.appendSourceCodeId({ batch });
        worker.markPerformance('start-upsert-person');
        await personWorker.appendPersonId({ batch, inputId });
        worker.markPerformance('start-append-entry');
        await worker.appendEntryId({ inputId, batch });
        worker.markPerformance('end-batch');
        batch.forEach((b) => {
          const {
            id, ts, entry_type_id, person_id, source_code_id,
          } = b;
          delete b.identifiers;
          delete b.source_input_id;// we already know these
          delete b.input_id;
          // eslint-disable-next-line prefer-destructuring
          if (b.email) b.email_domain = b.email.split('@').slice(-1)[0].toLowerCase();
          if (initialItem.email_hash_v1 === undefined) delete b.email_hash_v1;
          if (initialItem.phone_hash_v1 === undefined) delete b.phone_hash_v1;

          const withId = {
            id, ts, entry_type_id, person_id, source_code_id, ...b,
          };
          idStream.write(withId);
        });

        return cb();
      },
    }),
  );
  await idStream.end();

  await new Promise((resolve) => { idFileStream.on('finish', resolve); });

  await fsp.rename(`${outputFile}${processId}`, `${outputFile}`);

  return { records, idFilename: `${outputFile}` };
};

Worker.prototype.id.metadata = {
  options: {
    filename: {},
    defaultEntryType: {
      description: 'Default entry type if not specified in the file',
    },
  },
};

Worker.prototype.loadTimeline = async function (options) {
  if (!options.inputId) throw new Error('loadTimeline requires and inputId');
  const fileWorker = new FileWorker(this);
  const { stream: inputStream } = await fileWorker.getStream(options);
  const required = ['ts', 'person_id', 'entry_type_id', 'source_code_id'];
  const stream = inputStream.pipe(
    new Transform({
      objectMode: true,
      async transform(item, enc, cb) {
        if (!uuidRegex.test(item.id)) {
          throw new Error('loadTimeline requires items to have a uuid style id');
        }
        const missing = required.filter((k) => item[k] === undefined);
        if (missing.length > 0) throw new Error(`loadTimeline is missing required fields ${missing.join(',')}`);
        cb(null, item);
      },
    }),
  );

  return this.insertFromStream({
    table: 'timeline', stream, upsert: true, defaults: { input_id: options.inputId },
  });
};
Worker.prototype.loadTimeline.metadata = {
  options: {
    stream: {},
    filename: {},
  },
};

Worker.prototype.loadTimelineExtension = async function (options) {
  const fileWorker = new FileWorker(this);
  const { stream } = await fileWorker.getStream(options);
  return this.insertFromStream({ table: options.table, stream, upsert: true });
};
Worker.prototype.loadTimelineExtension.metadata = {
  options: {
    stream: {},
    filename: {},
  },
};

module.exports = Worker;
