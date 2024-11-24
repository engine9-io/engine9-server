const util = require('node:util');
const { pipeline } = require('node:stream/promises');
const fs = require('node:fs').promises;
const path = require('node:path');
const { Transform } = require('node:stream');
const { mkdirp } = require('mkdirp');
const debug = require('debug')('InputWorker');
const SQLiteWorker = require('./sql/SQLiteWorker');
const PersonWorker = require('./PersonWorker');
const FileWorker = require('./FileWorker');

function Worker(worker) {
  PersonWorker.call(this, worker);
}

util.inherits(Worker, PersonWorker);

/*
  An input storage db gets or creates an input storage file,
  currently a SQLite database with a timestamp
*/
Worker.prototype.getInputStorageDB = async function ({ inputId, datePrefix }) {
  const store = process.env.ENGINE9_TIMELINE_STORE;
  if (!store) throw new Error('No ENGINE9_TIMELINE_STORE configured');
  const dir = [store, datePrefix, inputId].join(path.sep);
  await (mkdirp(dir));
  const sqliteFile = `${dir + path.sep}timeline.sqlite`;
  const exists = await fs.stat(sqliteFile).then(() => true).catch(() => false);
  const db = new SQLiteWorker({ accountId: this.accountId, sqliteFile });
  if (exists) {
    return db;
  }

  await db.query('BEGIN;');
  await db.query(`create table timeline(
          id text not null primary key,
          ts integer not null,
          input_id text not null,
          entry_type_id smallint not null,
          person_id bigint not null,
          source_code_id bigint not null,
          created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )`);
  await db.query('CREATE INDEX timeline_ts ON timeline (ts)');
  await db.query('CREATE INDEX timeline_person_id ON timeline (person_id)');
  await db.query(`create table timeline_details(
          id text not null primary key,
          details jsonb not null      
        )`);
  await db.query('COMMIT;');

  return db;
};

Worker.prototype.load = async function (options) {
  const worker = this;
  const { pluginId } = options;
  if (!pluginId) throw new Error('load requires a pluginId');
  const fileWorker = new FileWorker(this);
  const batcher = this.getBatchTransform({ batchSize: 300 }).transform;
  const outputFiles = {};
  await pipeline(
    (await fileWorker.fileToObjectStream(options)).stream,
    batcher,
    new Transform({
      objectMode: true,
      async transform(batch, encoding, cb) {
        // eslint-disable-next-line no-underscore-dangle
        if (batch[0]?._is_placeholder) {
          return cb(null);
        }
        await worker.appendInputId({ pluginId, batch });
        await worker.appendEntryTypeId({ batch });
        await worker.appendSourceCodeId({ batch });
        await worker.appendPersonId({ batch });
        await worker.appendEntryId({ pluginId, batch });

        const output = await worker.upsertTimelineInputFile({ batch });
        // const { recordCounts }
        debug(output.files);
        Object.entries(output.files).forEach(([k, r]) => {
          outputFiles[k] = (outputFiles[k] || 0) + r;
        });
        return cb();
      },
    }),
  );
  return outputFiles;
};

Worker.prototype.load.metadata = {
  options: {
    filename: {},
  },
};

Worker.prototype.upsertTimelineInputFile = async function ({ batch }) {
  const timelineFiles = {};
  // Split the
  // eslint-disable-next-line no-restricted-syntax
  for (const o of batch) {
    const { ts, input_id: inputId } = o;
    const datePrefix = new Date(ts).getFullYear();// use the date prefix for the id
    let info = timelineFiles[`${datePrefix}:${inputId}`];
    if (!info) {
      // eslint-disable-next-line no-await-in-loop
      const db = await this.getInputStorageDB({ inputId, datePrefix });
      info = { records: 0, db, array: [] };
      timelineFiles[`${datePrefix}:${inputId}`] = info;
    }
    info.array.push(o);
  }
  const output = { files: {} };
  // eslint-disable-next-line no-restricted-syntax
  for (const [filename, { db, array }] of Object.entries(timelineFiles)) {
    // eslint-disable-next-line no-await-in-loop
    const resultArray = await db.upsertArray({ table: 'timeline', array });

    output.files[filename] = resultArray.length;
  }
  return output;
};

module.exports = Worker;
