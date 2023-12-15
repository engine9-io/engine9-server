const util = require('util');
const Knex = require('knex');
const { SchemaInspector } = require('knex-schema-inspector');
const { bool } = require('../utilities');

const BaseWorker = require('./BaseWorker');

require('dotenv').config({ path: '.env' });

function Worker(worker) {
  BaseWorker.call(this, worker);
}

util.inherits(Worker, BaseWorker);
Worker.metadata = {
  alias: 'sql',
};

Worker.prototype.connect = async function connect() {
  if (this.knex) return this.knex;
  const { accountId } = this;
  if (!this.accountId) throw new Error('accountId is required');

  let config = null;
  if (accountId === 'steamengine') {
    const s = process.env.STEAMENGINE_DATABASE_CONNECTION_STRING;
    if (!s) throw new Error('Could not find environment variable \'STEAMENGINE_DATABASE_CONNECTION_STRING\'');
    config = {
      client: 'mysql2',
      connection: process.env.STEAMENGINE_DATABASE_CONNECTION_STRING,
    };
  } else {
    throw new Error('Unsupported account:', accountId);
  }

  this.knex = Knex(config);
  return this.knex;
};
Worker.prototype.ok = async function f() {
  const knex = await this.connect();
  return knex.raw('select 1');
};
Worker.prototype.ok.metadata = {
  options: {},
};

Worker.prototype.tables = async function f() {
  const knex = await this.connect();
  const inspector = SchemaInspector(knex);
  return inspector.tables();
};
Worker.prototype.tables.metadata = {
  options: {},
};

const tableNameMatch = /^[a-zA-Z0-9_]+$/;
Worker.prototype.escapeTable = function escapeTable(t) {
  if (!t.match(tableNameMatch)) throw new Error(`Invalid table name: ${t}`);
  return t;
};

Worker.prototype.indexes = async function indexes({ table, unique, primary }) {
  let sql = `SELECT index_name,group_concat(column_name order by seq_in_index) as fields, not(non_unique) as \`unique\` 
    FROM INFORMATION_SCHEMA.STATISTICS where TABLE_SCHEMA = database() 
    and table_name='${this.escapeTable(table)}'`;
  if (bool(unique, false)) {
    sql += ' and non_unique=0';
  }
  if (bool(primary, false)) {
    sql += " and index_name='PRIMARY'";
  }
  sql += ' group by table_name,index_name,`unique`';
  const knex = await this.connect();

  const d = await knex.raw(sql);

  return {
    table,
    indexes: d[0].map((i) => ({
      index_name: i.INDEX_NAME || i.index_name,
      fields: i.fields,
      primary: (i.INDEX_NAME || i.index_name) === 'PRIMARY',
      unique: !!i.unique,
    })),
  };
};

Worker.prototype.indexes.metadata = {
  options: {
    table: { required: true },
    unique: { description: 'Only include the unique indexes?  Default no' },
    primary: { description: 'Only include the primary index?  Default no' },
  },
};

Worker.prototype.describe = async function describe({ table }) {
  const knex = await this.connect();
  const inspector = SchemaInspector(knex);
  const columns = await inspector.columns(table);
  return {
    columns: (await Promise.all(columns.map(
      async ({ column }) => inspector.columnInfo(table, column),
    ))),
    primary: await inspector.primary(table),
  };
};

Worker.prototype.describe.metadata = {
  options: {
    table: { required: true },
  },
};

Worker.prototype.stream = async function describe({ sql }) {
  const knex = await this.connect();
  return knex.raw(sql).stream();
};

Worker.prototype.stream.metadata = {
  options: {
    sql: { required: true },
  },
};

module.exports = Worker;
