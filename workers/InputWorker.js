const { performance } = require('node:perf_hooks');
const util = require('node:util');
const { pipeline } = require('node:stream/promises');
const fs = require('node:fs').promises;
const path = require('node:path');
const { Transform } = require('node:stream');
const { mkdirp } = require('mkdirp');
const debug = require('debug')('InputWorker');
const SQLWorker = require('./SQLWorker');
const SQLiteWorker = require('./sql/SQLiteWorker');
const PersonWorker = require('./PersonWorker');
const FileWorker = require('./FileWorker');
const { bool } = require('../utilities');

/*
const perfObserver = new PerformanceObserver((items) => {
  items.getEntries().forEach((entry) => {
    debugPerformance('%o', entry);
  });
});

perfObserver.observe({ entryTypes: ['measure'], buffer: true });
*/

function Worker(worker) {
  PersonWorker.call(this, worker);
}

util.inherits(Worker, PersonWorker);

/*
  An input storage db gets or creates an input storage file,
  currently a SQLite database with a timestamp
*/
const store = process.env.ENGINE9_STORED_INPUT_PATH;
if (!store) throw new Error('No ENGINE9_STORED_INPUT_PATH configured');
Worker.prototype.getStoredInputDB = async function ({ inputId, datePrefix }) {
  const dir = [store, this.accountId, datePrefix, inputId].join(path.sep);
  this.storedInputCache = this.storedInputCache || {};
  await (mkdirp(dir));
  const sqliteFile = `${dir + path.sep}timeline.sqlite`;
  let output = this.storedInputCache[sqliteFile];
  if (output) return output;
  const exists = await fs.stat(sqliteFile).then(() => true).catch(() => false);
  const db = new SQLiteWorker({ accountId: this.accountId, sqliteFile });
  if (exists) {
    output = { filename: sqliteFile, db };
    this.storedInputCache[sqliteFile] = output;
    return output;
  }

  await db.query('BEGIN;');
  await db.query(`create table timeline(
          id text not null primary key,
          ts integer not null,
          ${/* input_id text not null, */''}
          entry_type_id smallint not null,
          person_id bigint not null,
          source_code_id bigint not null,
          created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )`);
  await db.query('CREATE INDEX timeline_ts ON timeline (ts)');
  await db.query('CREATE INDEX timeline_person_id ON timeline (person_id)');
  await db.query(`create table timeline_detail(
          id text not null primary key,
          details jsonb not null      
        )`);
  await db.query('COMMIT;');

  output = { filename: sqliteFile, db };
  this.storedInputCache[sqliteFile] = output;
  return output;
};
Worker.prototype.destroy = async function () {
  Object.entries(this.storedInputCache || {}).forEach(([, { db }]) => {
    db.destroy();
  });
};

Worker.prototype.upsertStoredInputFile = async function ({ batch }) {
  const timelineFiles = {};
  // Split the
  // eslint-disable-next-line no-restricted-syntax
  for (const o of batch) {
    const { ts, input_id: inputId } = o;
    const datePrefix = new Date(ts).getFullYear();// use the date prefix for the id
    let info = timelineFiles[`${datePrefix}:${inputId}`];
    if (!info) {
      // eslint-disable-next-line no-await-in-loop
      const { filename, db } = await this.getStoredInputDB({ inputId, datePrefix });
      info = {
        filename, records: 0, db, array: [],
      };
      timelineFiles[`${datePrefix}:${inputId}`] = info;
    }
    info.array.push(o);
  }
  const output = { };
  const ignoreFields = {
    id: 1,
    ts: 1,
    input_id: 1,
    entry_type_id: 1,
    entry_type: 1,
    person_id: 1,
    source_code: 1,
    source_code_id: 1,
  };

  // eslint-disable-next-line no-restricted-syntax
  for (const [dbKey, timelineObj] of Object.entries(timelineFiles)) {
    const { db, array, filename } = timelineObj;
    try {
      // eslint-disable-next-line no-await-in-loop
      await db.upsertArray({ table: 'timeline', array });
      // we don't need to use the results for anything
      const detailArray = array.map((d) => {
        const details = {};
        let include = false;
        Object.entries(d).forEach(([k, v]) => {
          if (!ignoreFields[k]) {
            details[k] = v;
            include = true;
          }
        });
        return include ? { id: d.id, details } : false;
      }).filter(Boolean);
      if (detailArray.length > 0) {
        // eslint-disable-next-line no-await-in-loop
        await db.upsertArray({ table: 'timeline_detail', array: detailArray });
        // we don't need to use the results for anything
      }

      output[dbKey] = { filename, records: array.length };
    } catch (e) {
      debug(`Error for key ${dbKey} in filename ${filename}`);
      throw e;
    } finally {
      // we could reuse this
      // db.destroy();
    }
  }
  return output;
};
/*
  Loads a file from an input to the database
*/
Worker.prototype.loadToTimeline = async function (options) {
  const { inputId, datePrefix } = options;
  const { db } = await this.getStoredInputDB({ inputId, datePrefix });
  // const sqlWorker = new SQLWorker({ accountId: this.accountId });
  const { stream } = db.stream('select * from timeline');
  if (!this.sqlWorker) this.sqlWorker = new SQLWorker(this);

  return this.sqlWorker.insertFromStream({ upsert: true, stream });
};
Worker.prototype.markPerformance = function (name) {
  this.performanceNames = this.performanceNames || {};
  this.performanceNames[name] = true;
  performance.mark(name);
};

Worker.prototype.load = async function (options) {
  const worker = this;

  const { pluginId, defaultEntryType, filename } = options;
  if (!pluginId) throw new Error('load requires a pluginId');
  const remoteInputId = filename.split(path.sep).pop();
  const loadTimeline = bool(options.loadTimeline, false);
  const fileWorker = new FileWorker(this);
  const batcher = this.getBatchTransform({ batchSize: 300 }).transform;
  const outputFiles = {};
  let inputRecords = 0;
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
        inputRecords += batch.length;
        if (batches % 10 === 0) debug(`Processed ${batches} batches, ${inputRecords} records`);
        worker.markPerformance('start-append-id');
        await worker.appendInputId({ pluginId, remoteInputId, batch });
        worker.markPerformance('start-entry-type-id');
        await worker.appendEntryTypeId({ batch, defaultEntryType });
        worker.markPerformance('start-source-code-id');
        await worker.appendSourceCodeId({ batch });
        worker.markPerformance('start-upsert-person');
        await worker.upsertPersonBatch({ batch });
        worker.markPerformance('start-append-entry');
        await worker.appendEntryId({ pluginId, batch });
        worker.markPerformance('start-upsert-stored-input');
        const output = await worker.upsertStoredInputFile({ batch });
        if (loadTimeline) {
          worker.markPerformance('start-load_timeline');
          await worker.insertFromStream({ table: 'timeline', upsert: true, stream: batch });
        }

        Object.entries(output).forEach(([k, r]) => {
          outputFiles[k] = outputFiles[k] || { filename: r.filename, records: 0 };
          outputFiles[k].records += (r.records || 0);
        });
        worker.markPerformance('end-all');
        return cb();
      },
    }),
  );
  const keys = Object.keys(this.performanceNames || {});
  for (let i = 0; i < keys.length - 1; i += 1) {
    const k = keys[i];
    // eslint-disable-next-line no-await-in-loop
    await performance.measure(`${k} -> ${keys[i + 1]}`, k, keys[i + 1]);
  }

  return { inputRecords, outputFiles };
};

Worker.prototype.load.metadata = {
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

module.exports = Worker;
