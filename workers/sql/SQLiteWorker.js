const util = require('node:util');
const { pipeline } = require('node:stream/promises');
const { Transform } = require('node:stream');

const Knex = require('knex');
const debug = require('debug')('debug info');
const { parse: parseUUID } = require('uuid');

const FileWorker = require('../FileWorker');

const SQLWorker = require('../SQLWorker');

function Worker(worker) {
  SQLWorker.call(this, worker);
  this.sqliteFile = worker.sqliteFile;
  if (!this.sqliteFile) {
    debug('Warning, sqliteFile should be specified in a constructor');
  }
}

util.inherits(Worker, SQLWorker);

Worker.prototype.info = function () {
  return {
    driver: 'better-sqlite3',
    dialect: this.dialect,
    filename: this.sqliteFile,
  };
};

Worker.prototype.connect = async function connect(options = {}) {
  const worker = this;
  if (this.knex) return this.knex;
  this.driver = 'better-sqlite3';
  this.sqliteFile = this.sqliteFile || options.sqliteFile;
  if (!this.sqliteFile) {
    debug('No sqliteFile in ', options);
    throw new Error(`database sqliteFile must be specified, not found in ${Object.keys(options)}`);
  }

  const authConfig = {
    client: 'better-sqlite3',
    connection: {
      filename: this.sqliteFile,
    },
    useNullAsDefault: true, // we have to add this as SQLite doesn't support default values
    pool: {
      afterCreate(conn, done) {
        worker.raw_connection = conn;
        conn.pragma('journal_mode = WAL');
        conn.pragma('synchronous = OFF');
        done();
      },
    },
  };

  this.knex = Knex(authConfig);

  return this.knex;
};

// This is engine dependent
Worker.prototype.parseQueryResults = function ({ results }) {
  let data; let records; let modified; let columns;
  if (Array.isArray(results)) {
    data = results;
    records = data.affectedRows;
    modified = data.changedRows;
  } else {
  // probably not a select
    data = [];
    records = results.changes;
    modified = results.changes;
    columns = [];
  }

  return {
    data, records, modified, columns,
  };
};

function chunkArray(arr, size) {
  const chunkedArray = [];
  for (let i = 0; i < arr.length; i += size) {
    const c = arr.slice(i, i + size);
    chunkedArray.push(c);
  }
  return chunkedArray;
}

Worker.prototype.describe = async function describe({ table }) {
  const desc = await this.query(`PRAGMA table_info(${table})`);
  if (desc.data.length === 0) {
    throw new Error(`Could not find table ${table} in file ${this.sqliteFile}`);
  }

  return {
    columns: desc.data.map((d) => ({
      name: d.name,
      column_type: d.type,
      // length: d.CHARACTER_MAXIMUM_LENGTH,
      nullable: !d.notnull,
      default_value: d.dflt_value,
    // auto_increment: (d.EXTRA || '').toUpperCase().indexOf('AUTO_INCREMENT') >= 0,
    })),
  };
};

Worker.prototype.onDuplicate = function () { return 'on conflict do update set'; };
Worker.prototype.onDuplicateFieldValue = function (f) { return `excluded.${f}`; };

Worker.prototype.ensureTimelineSchema = async function ({ includeEmailDomain } = {}) {
  try {
    await this.describe({ table: 'timeline' });
    return { exists: true };
  } catch (e) {
    // do nothing, it doesn't exist
  }

  await this.query(`create table timeline(
    id BLOB PRIMARY KEY,
    ts INTEGER not null,
    entry_type_id INTEGER not null,${''/* technically a smallint */}
    person_id INTEGER not null,${''/* technically a bigint */}
    source_code_id INTEGER not null${''/* technically a bigint */}
    ${includeEmailDomain ? ',email_domain TEXT' : ''}
  )`);
  return { created: true };
};

Worker.prototype.loadBatchToTimeline = async function ({
  batch,
}) {
  const includeEmailDomain = !!batch?.[0]?.email_domain;
  await this.ensureTimelineSchema({ includeEmailDomain });

  const output = { sqliteFile: this.sqliteFile, records: 0 };
  const fields = {
    id: (a) => {
      try {
        return parseUUID(a.id);
      } catch (e) {
        throw new Error(`Invalid UUID:${a.id}`);
      }
    },
    ts: (a) => new Date(a.ts).getTime(),
    entry_type_id: (a) => parseInt(a.entry_type_id, 10),
    person_id: (a) => parseInt(a.person_id, 10),
    source_code_id: (a) => parseInt(a.source_code_id, 10),
  };
  if (includeEmailDomain) fields.email_domain = ((a) => (a.email_domain || '').toLowerCase());

  const chunkSize = 100;
  try {
    /* Uses the raw connection for transactions and prepared statements */
    this.raw_connection.prepare('BEGIN TRANSACTION').run();
    const stmt = this.raw_connection.prepare(`insert into timeline (${Object.keys(fields).join(',')})
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
      const rstmt = this.raw_connection.prepare(`insert into timeline (${Object.keys(fields).join(',')})
    values ${new Array(remainder.length).fill(`(${Object.keys(fields).map(() => '?').join(',')})`).join(',')}
    on conflict do nothing`);
      rstmt.run(...remainder);
      output.records += remainder.length;
    }
    this.raw_connection.prepare('END TRANSACTION').run();
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
  Load data to a timeline table
*/
Worker.prototype.loadTimeline = async function (options) {
  const worker = this;

  const {
    filename,
  } = options;

  const fileWorker = new FileWorker(this);
  const batcher = this.getBatchTransform({ batchSize: 300 }).transform;

  let inputRecords = 0;

  let batches = 0;

  const output = { sqliteFile: this.sqliteFile, records: 0 };
  const columnNames = ['id', 'ts', 'person_id', 'entry_type_id', 'source_code_id', 'email_domain'];// pull email domain, IF it's there

  await pipeline(
    (await fileWorker.fileToObjectStream({ filename, columns: columnNames })).stream,
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

        const { records } = await worker.loadBatchToTimeline({ batch });
        output.records += records;

        worker.markPerformance('end-batch');
        return cb();
      },
    }),
  );
  output.columns = (await worker.describe({ table: 'timeline' })).columns;

  return output;
};

Worker.prototype.loadTimeline.metadata = {
  options: {
    filename: { required: true },
  },
};

Worker.prototype.sizes = async function (options) {
  await this.connect(options);
  return this.query('select name,sum("pgsize") from dbstat group by name');
};
Worker.prototype.sizes.metadata = {
  options: {
    filename: {},
  },
};

/*
  Loads a file from an inputDB to the database
*/
Worker.prototype.loadToWarehouseTimeline = async function (options) {
  const { inputId } = options;
  if (!inputId) throw new Error('No inputId');

  const sqlWorker = new SQLWorker(this);
  const stream = await this.stream({ sql: `select *,${this.escapeValue(inputId)} as input_id from timeline limit 1000` });

  return sqlWorker.insertFromStream({ table: 'timeline', upsert: true, stream });
};
Worker.prototype.loadToWarehouseTimeline.metadata = {
  options: {
    inputId: { required: true },
  },
};
Worker.prototype.destroy = async function () {
  await this.knex.destroy();
  // await this.raw_connection.close();
};

module.exports = Worker;
