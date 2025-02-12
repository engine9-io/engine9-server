/* eslint-disable no-await-in-loop */
/* eslint-disable camelcase */

const util = require('node:util');
const { pipeline } = require('node:stream/promises');
const { Transform } = require('node:stream');
const fs = require('node:fs');

const fsp = fs.promises;
const path = require('node:path');

const zlib = require('node:zlib');
const parquet = require('@dsnp/parquetjs');
const csv = require('csv');
const { mkdirp } = require('mkdirp');
const debug = require('debug')('InputWorker');
const { uuidRegex, getTempFilename } = require('@engine9/packet-tools');
// const SQLWorker = require('./SQLWorker');
const SQLiteWorker = require('./sql/SQLiteWorker');
const PluginBaseWorker = require('./PluginBaseWorker');
const FileWorker = require('./FileWorker');
const S3Worker = require('./file/S3Worker');
const PersonWorker = require('./PersonWorker');
const { analyzeTypeToParquet } = require('../utilities');

function Worker(worker) {
  PluginBaseWorker.call(this, worker);
  /*
    Input files can be stored in a directory, or s3
  */
  this.store_path = process.env.ENGINE9_STORED_INPUT_PATH;
  if (!this.store_path) throw new Error('No ENGINE9_STORED_INPUT_PATH configured');
}

util.inherits(Worker, PluginBaseWorker);

/*
  Sometimes we like to use a SQLite DB for calculating stats
*/
Worker.prototype.getTimelineInputSQLiteDB = async function ({
  sqliteFile,
  inputId,
  includeEmailDomain = false,
}) {
  if (!sqliteFile && !inputId) throw new Error('getTimelineInputSQLiteDB requires a sqliteFile or inputId');
  let dbpath = sqliteFile;
  this.timelineSQLiteDBCache = this.timelineSQLiteDBCache || {};
  if (!dbpath) {
    const dir = [this.store_path, this.accountId, inputId.slice(0, 4), inputId].join(path.sep);
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

Worker.prototype.id = async function (options) {
  const worker = this;

  const {
    inputId, defaultEntryType, filename,
  } = options;
  if (!inputId) {
    throw new Error('id requires an inputId');
  }
  let { pluginId } = options;
  if (!pluginId) {
    const { data: plugin } = await this.query({ sql: 'select plugin_id from input where id=?', values: [inputId] });
    pluginId = plugin?.[0]?.plugin_id;
    if (!pluginId) throw new Error(`Could not find pluginId for inputId=${inputId}`);
  }

  const processId = `.${new Date().getTime()}.processing`;

  const fileWorker = new FileWorker(this);
  const personWorker = new PersonWorker(this);
  const { fields } = await fileWorker.analyze({ filename });

  const initialSchema = {
    // So, this is a UUID, but it's a pain to read them in the files, so do string instead
    // until node.js supports the UUID type natively
    // id: { parquetType: 'BYTE_ARRAY',parquetMap: (v) => v  },
    id: { parquetType: 'UTF8', parquetMap: (v) => v },
    ts: { parquetType: 'TIMESTAMP_MILLIS', parquetMap: (v) => new Date(v) },
    entry_type_id: { parquetType: 'INT32', parquetMap: (v) => parseInt(v, 10) },
    person_id: { parquetType: 'INT64', parquetMap: (v) => parseInt(v, 10) },
    source_code_id: { parquetType: 'INT64', parquetMap: (v) => parseInt(v, 10) },
  };
  if (fields.find((d) => d.name === 'email')) {
    initialSchema.email_domain = { parquetType: 'UTF8', parquetMap: (v, obj) => (obj.email || '').split('@').slice(-1)[0].toLowerCase() };
  }

  const fieldMap = fields.reduce((a, b) => {
    const pq = analyzeTypeToParquet(b.type);
    if (a[b.name]) return a;
    // ignore these, as they're specified by id, but keep the person info just in case
    if (['identifiers', 'entry_type', 'source_code'].indexOf(b.name) >= 0) return a;
    a[b.name] = {
      type: b.type,
      parquetType: pq.type,
      parquetMap: pq.map,
    };
    return a;
  }, initialSchema);

  const parquetSchemaDefinition = {};
  Object.entries(fieldMap).forEach(([name, obj]) => {
    parquetSchemaDefinition[name] = { type: obj.parquetType };
  });

  const parquetSchema = new parquet.ParquetSchema(parquetSchemaDefinition);

  let outputFile;
  if (filename.indexOf('/') === 0) {
    outputFile = `${filename}.with_ids.parquet`;
  } else {
    outputFile = await getTempFilename({ postfix: '.with_ids.parquet' });
  }

  const writer = await parquet.ParquetWriter.openFile(parquetSchema, `${outputFile}${processId}`, {
    bloomFilters: [
      {
        column: 'ts',
        numFilterBytes: 1024,
      },
    ],
  });

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

        batches += 1;
        records += batch.length;
        if (batches % 10 === 0) debug(`Processed ${batches} batches, ${records} records`);
        worker.markPerformance('start-entry-type-id');
        await worker.appendEntryTypeId({ batch, defaultEntryType });
        worker.markPerformance('start-source-code-id');
        await worker.appendSourceCodeId({ batch });
        worker.markPerformance('start-upsert-person');
        await personWorker.loadPeople({ stream: batch, pluginId, inputId });
        worker.markPerformance('start-append-entry');
        await worker.appendEntryId({ inputId, batch });
        worker.markPerformance('end-batch');
        // parquetjs requires the appendRow to be completed before subsequent calls
        // eslint-disable-next-line no-restricted-syntax
        for (const b of batch) {
          const row = {};
          Object.entries(fieldMap).forEach(([name, { parquetMap }]) => {
            row[name] = parquetMap(b[name], b);
          });
          await writer.appendRow(row);
        }
        debug(`Wrote ${records} records`);

        return cb();
      },
    }),
  );
  await writer.close();
  debug(`Finished writing ${outputFile}`);

  await fsp.rename(`${outputFile}${processId}`, `${outputFile}`);
  // copy it back to s3
  if (filename.indexOf('s3://') === 0) {
    debug('Copying to s3');
    const s3 = new S3Worker(this);
    const file = filename.split('/').pop();
    const directory = filename.split('/').slice(0, -1).join('/');
    const s3Results = await s3.put({ filename: outputFile, directory });
    return {
      records, idFilename: `${directory}/${file}`, sourceIdFilename: outputFile, s3Results,
    };
  }
  return { records, idFilename: outputFile };
};

Worker.prototype.id.metadata = {
  options: {
    filename: {},
    defaultEntryType: {
      description: 'Default entry type if not specified in the file',
    },
  },
};

/*
  Add ids to an input file -- presumed to already be split by inputs
*/
Worker.prototype.idCSV = async function (options) {
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

Worker.prototype.idCSV.metadata = {
  options: {
    filename: {},
    defaultEntryType: {
      description: 'Default entry type if not specified in the file',
    },
  },
};

/*
//Sample multi-file inputs
fileArray: [
    {
      inputId: '2b335df4-940d-52c9-94d2-b3a06883bc1b',
      remoteInputId: 'abc1230',
      source_filename:
      '/var/folders/59/2025-02-09/2025_02_09_20_01_07_921_3497.input.csv.gz',
      filename: 's3://engine9-accounts/dev/stored_input/2b33/2b335df4-940d-52c9-94d2-b3a06883bc1b/2025_02_09_20_01_07_921_3497.input.csv.gz',
      records: 4
    },
    {
      inputId: '16f8d68e-8c37-51b3-8c87-dc67ac305d69',
      remoteInputId: 'abc1232',
      source_filename: '/var/folders/59/T/dev/2025-02-09/2025_02_09_20_01_07_924_9112.input.csv.gz',
      filename: 's3://engine9-accounts/dev/stored_input/16f8/16f8d68e-8c37-51b3-8c87-dc67ac305d69/2025_02_09_20_01_07_924_9112.input.csv.gz',
      records: 3
    }
  ],

*/
Worker.prototype.idAndLoadFiles = async function (options) {
  let arr = options.fileArray;
  if (typeof arr === 'string') arr = JSON.parse(arr);
  if (!Array.isArray(arr))arr = [options];
  const output = [];
  // eslint-disable-next-line no-restricted-syntax
  for (const o of arr) {
    const { inputId, filename } = o;
    const { idFilename } = await this.id({ filename, inputId });
    const timeline = await this.loadTimeline({ filename: idFilename, inputId });
    let details = null;
    if (options.detailsTable) {
      details = await this.loadTimelineDetails({ filename, inputId });
    }
    output.push({
      inputId,
      filename,
      timeline,
      details,
    });
  }

  return output;
};
Worker.prototype.idAndLoadFiles.metadata = {
  options: {
    fileArray: {},
    filename: {},
    inputId: {},
    detailsTable: {},
  },
};

Worker.prototype.loadTimeline = async function (options) {
  if (!options.inputId) throw new Error('loadTimeline requires an inputId');
  const fileWorker = new FileWorker(this);
  const columns = ['id', 'ts', 'person_id', 'entry_type_id', 'source_code_id'];
  const { stream: inputStream } = await fileWorker.stream({ ...options, columns });

  const stream = inputStream.pipe(
    new Transform({
      objectMode: true,
      async transform(item, enc, cb) {
        if (!uuidRegex.test(item.id)) {
          throw new Error('loadTimeline requires items to have a uuid style id');
        }
        const missing = columns.filter((k) => item[k] === undefined);
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

Worker.prototype.loadTimelineDetails = async function (options) {
  const { columns } = await this.describe(options);

  const columnNames = columns.map((d) => d.name);

  const fileWorker = new FileWorker(this);
  const { stream } = await fileWorker.stream({ ...options, columns: columnNames });
  return this.insertFromStream({ table: options.table, stream, upsert: true });
};
Worker.prototype.loadTimelineDetails.metadata = {
  options: {
    stream: {},
    filename: {},
    columns: {},
  },
};

module.exports = Worker;
