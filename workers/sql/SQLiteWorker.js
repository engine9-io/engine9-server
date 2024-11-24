const util = require('node:util');

const Knex = require('knex');
const SQLWorker = require('../SQLWorker');

function Worker(worker) {
  SQLWorker.call(this, worker);
  this.sqliteFile = worker.sqliteFile;
}

util.inherits(Worker, SQLWorker);

Worker.prototype.connect = async function connect() {
  if (this.knex) return this.knex;
  if (!this.sqliteFile) throw new Error('database filename must be specified');

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

module.exports = Worker;
