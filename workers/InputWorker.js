/* eslint-disable camelcase */

const util = require('node:util');
const { pipeline } = require('node:stream/promises');
const fs = require('node:fs');

const fsp = fs.promises;
const path = require('node:path');
const { Transform } = require('node:stream');
const zlib = require('node:zlib');
const csv = require('csv');
const { mkdirp } = require('mkdirp');
const debug = require('debug')('InputWorker');
const SQLite3 = require('better-sqlite3');
const { parse: parseUUID } = require('uuid');
const { getTempFilename, getUUIDv7 } = require('@engine9/packet-tools');
const SQLWorker = require('./SQLWorker');
const SQLiteWorker = require('./sql/SQLiteWorker');
const PluginBaseWorker = require('./PluginBaseWorker');
const FileWorker = require('./FileWorker');

function Worker(worker) {
  PluginBaseWorker.call(this, worker);
}

util.inherits(Worker, PluginBaseWorker);

/*
  The input id is either stored in the database, or generated and
stored
*/
Worker.prototype.getInputId = async function (opts) {
  const {
    inputId, pluginId, remoteInputId, inputType = 'unknown',
  } = opts;
  if (inputId) return inputId;
  if (!pluginId || !remoteInputId) throw new Error('pluginId and remoteInputId are both required');
  const { data } = await this.query({ sql: 'select * from input where plugin_id=? and remote_input_id=?', values: [pluginId, remoteInputId] });
  if (data.length > 0) return data[0].id;
  const { data: plugin } = await this.query({ sql: 'select * from plugin where plugin_id=?', values: [pluginId] });
  if (plugin.length === 0) throw new Error(`No such plugin:${pluginId}`);
  const id = getUUIDv7(new Date());
  await this.insertFromStream({
    table: 'input',
    stream: [{
      id,
      plugin_id: pluginId,
      remote_input_id: remoteInputId,
      input_type: inputType,
    },
    ],
  });

  return id;
};

/*
  An input storage db gets or creates an input storage DB file,
  currently a SQLite database with a timestamp
*/
const store = process.env.ENGINE9_STORED_INPUT_PATH;
if (!store) throw new Error('No ENGINE9_STORED_INPUT_PATH configured');
Worker.prototype.getStoredInputDB = async function ({
  sqliteFile,
  inputId,
  includeEmailDomain = false,
}) {
  if (!sqliteFile && !inputId) throw new Error('getStoredInputDB requires a sqliteFile or inputId');
  let dbpath = sqliteFile;
  this.storedInputCache = this.storedInputCache || {};
  if (!dbpath) {
    const dir = [store, this.accountId, inputId.slice(0, 4), inputId].join(path.sep);
    await (mkdirp(dir));
    dbpath = `${dir + path.sep}input.db`;
  }

  let output = this.storedInputCache[dbpath];
  if (output) return output;
  const exists = await fsp.stat(dbpath).then(() => true).catch(() => false);
  const db = new SQLite3(dbpath);
  if (exists) {
    output = { sqliteFile: dbpath, db };
    this.storedInputCache[dbpath] = output;
    return output;
  }

  db.prepare(`create table timeline(
          id blob PRIMARY KEY,
          ts integer not null,
          entry_type_id smallint not null,
          person_id bigint not null,
          source_code_id bigint not null
          ${includeEmailDomain ? ',email_domain text' : ''}
        )`).run();
  db.prepare('PRAGMA synchronous = OFF');
  output = { sqliteFile: dbpath, db };
  this.storedInputCache[dbpath] = output;
  return output;
};

Worker.prototype.destroy = async function () {
  Object.entries(this.storedInputCache || {}).forEach(([, { db }]) => {
    db.close();
  });
};

function chunkArray(arr, size) {
  const chunkedArray = [];
  for (let i = 0; i < arr.length; i += size) {
    const c = arr.slice(i, i + size);
    chunkedArray.push(c);
  }
  return chunkedArray;
}

Worker.prototype.loadBatchToInputDB = async function ({
  sqliteFile: sqliteFileOpt,
  inputId,
  batch,
}) {
  const includeEmailDomain = !!batch?.[0]?.email_domain;
  const { sqliteFile, db } = await this.getStoredInputDB({
    sqliteFile: sqliteFileOpt,
    inputId,
    includeEmailDomain,
  });

  const output = { sqliteFile, records: 0 };
  const fields = {
    id: (a) => parseUUID(a.id),
    ts: (a) => new Date(a.ts).getTime(),
    entry_type_id: (a) => parseInt(a.entry_type_id, 10),
    person_id: (a) => parseInt(a.person_id, 10),
    source_code_id: (a) => parseInt(a.source_code_id, 10),
  };
  if (includeEmailDomain) fields.email_domain = ((a) => (a.email_domain || '').toLowerCase());

  const chunkSize = 100;
  try {
    db.prepare('BEGIN TRANSACTION').run();
    const stmt = db.prepare(`insert into timeline (${Object.keys(fields).join(',')})
    values ${new Array(chunkSize).fill(`(${Object.keys(fields).map(() => '?').join(',')})`).join(',')}
    on conflict do nothing`);
    const parsedVals = batch.map((a) => (Object.entries(fields).map(([, f]) => f(a))));
    let remainder = null;
    chunkArray(parsedVals, chunkSize).forEach((chunk) => {
      if (chunk.length === chunkSize) {
        stmt.run(...chunk);
        output.records += chunk.length;
      } else {
        remainder = chunk;
      }
    });
    if (remainder) {
      const rstmt = db.prepare(`insert into timeline (id,ts,entry_type_id,person_id,source_code_id,email_domain)
    values ${new Array(remainder.length).fill('(?,?,?,?,?,?)').join(',')}
    on conflict do nothing`);
      rstmt.run(...remainder);
      output.records += remainder.length;
    }
    db.prepare('END TRANSACTION').run();
  } catch (e) {
    debug('Error for db file {sqliteFile}');
    throw e;
  } finally {
    // we could reuse this
    // db.destroy();
  }

  return output;
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

  const idStream = csv.stringify({ header: true });
  let outputFile;
  if (filename.indexOf('/') === 0) {
    outputFile = `${filename}.with_ids.csv.gz${processId}`;
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
        batch.forEach((b) => { b.source_input_id = b.source_input_id || inputId; });
        await worker.upsertPersonBatch({ batch });
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

  return { records, id_filename: `${outputFile}` };
};

Worker.prototype.id.metadata = {
  options: {
    filename: {},
    loadTimeline: {
      description: 'Whether to load the database as well as the file, default false',
    },
    defaultEntryType: {
      description: 'Default entry type if not specified in the file',
    },
  },
};

/*
  Load data to an inputDB
*/
Worker.prototype.loadInputDB = async function (options) {
  const worker = this;

  const {
    filename,
  } = options;
  let { sqliteFile } = options;

  const fileWorker = new FileWorker(this);
  const batcher = this.getBatchTransform({ batchSize: 300 }).transform;

  let inputRecords = 0;

  let batches = 0;
  if (!sqliteFile) {
    if (filename.indexOf('/') === 0) {
      sqliteFile = filename.split(path.sep).slice(0, -1).concat('input.db').join(path.sep);
    } else {
      sqliteFile = await getTempFilename({ postfix: '.input.db' });
    }
  }
  const output = { sqliteFile, records: 0 };
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
        inputRecords += batch.length;
        if (batches % 10 === 0) debug(`Processed ${batches} batches, ${inputRecords} records`);

        const { records } = await worker.loadBatchToInputDB({ sqliteFile, batch });
        output.records += records;

        worker.markPerformance('end-batch');
        return cb();
      },
    }),
  );

  return output;
};

Worker.prototype.loadInputDB.metadata = {
  options: {
    filename: { required: true },
    sqliteFile: { },
  },
};
/*
  Loads a file from an inputDB to the database
*/
Worker.prototype.loadInputDBToTimeline = async function (options) {
  const { sqliteFile, inputId } = options;
  const sqlWorker = new SQLiteWorker({ accountId: this.accountId, sqliteFile });
  if (!this.sqlWorker) this.sqlWorker = new SQLWorker(this);
  const stream = await sqlWorker.stream({ sql: `select *,${sqlWorker.escapeValue(inputId)} as input_id from timeline limit 1000` });

  return this.sqlWorker.insertFromStream({ table: 'timeline', upsert: true, stream });
};
Worker.prototype.loadInputDBToTimeline.metadata = {
  options: {
    sqliteFile: { required: true },
    inputId: { required: true },
  },
};

module.exports = Worker;
