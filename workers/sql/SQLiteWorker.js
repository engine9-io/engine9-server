const util = require('node:util');

const Knex = require('knex');
const debug = require('debug')('debug info');
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
  if (this.knex) return this.knex;
  this.driver = 'better-sqlite3';
  this.sqliteFile = options.filename || this.sqliteFile;
  if (!this.sqliteFile) {
    throw new Error('database filename must be specified');
  }

  const authConfig = {
    client: 'better-sqlite3',
    connection: {
      filename: this.sqliteFile,
    },
    pool: {
      afterCreate(conn, done) {
        conn.pragma('journal_mode = WAL');
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

Worker.prototype.sizes = async function (options) {
  await this.connect(options);
  return this.query('select name,sum("pgsize") from dbstat group by name');
};
Worker.prototype.sizes.metadata = {
  options: {
    filename: {},
  },
};

module.exports = Worker;
