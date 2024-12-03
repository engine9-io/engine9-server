const util = require('node:util');

const Knex = require('knex');
const debug = require('debug')('ClickHouseWorker');
const SQLWorker = require('../SQLWorker');

const clickhouseDialect = require('./dialects/ClickHouse');
const { relativeDate } = require('../../utilities');

function Worker(worker) {
  SQLWorker.call(this, worker);
  this.accountId = worker.accountId;
  this.dialect = clickhouseDialect;
}

util.inherits(Worker, SQLWorker);

Worker.prototype.connect = async function connect() {
  if (this.knex) return this.knex;
  if (!this.accountId) throw new Error('No accountId specified');

  const connString = process.env.ENGINE9_CLICKHOUSE_CONNECTION;
  if (!connString) {
    debug(Object.keys(process.env));
    throw new Error('ENGINE9_CLICKHOUSE_CONNECTION is a required environment variable');
  }
  this.knex = new Knex({
    client: 'mysql2', // use mysql protocol, as the only existing one has flaws with insert...select
    connection: () => connString + this.accountId,
  });
  return this.knex;
};

/*
  Try to determine a viable database structure from an analysis.  This needs to be more specific
  than the normal schema work as it must have specific column details based on ranges,
  whereas not every schema
*/

const stringNames = ['given_name', 'family_name', 'email_hash_v1', 'phone_hash_v1'];
Worker.prototype.deduceColumnDefinition = function ({
  name,
  type,
  // empty,
  min,
  max,
  // min_length,
  // max_length,
  distinct,
  // sample,
}) {
  if (!type) {
    throw new Error(`No type for field ${name}`);
  }

  const output = {
    name,
    isKnexDefinition: true, // use these directly, don't map through the standard types
    method: null,
    args: null,
    nullable: false,
    defaultValue: 0,
  };
  if (stringNames.indexOf(name) >= 0) {
    return Object.assign(output, { method: 'specificType', args: ['String'], defaultValue: '' });
  } if (type === 'int') {
    if (min === undefined || max === undefined) {
      // default to normal int
      output.method = 'int';
    } else if (min < 0) {
      if (min >= -128 && max <= 127) {
        // 1byte
        Object.assign(output, { method: 'specificType', args: ['Int8'] });
      } else if (min >= -32768 && max <= 32767) {
        // 2bytes
        Object.assign(output, { method: 'specificType', args: ['Int16'] });
      } else if (min >= -8388608 && max <= 8388607) {
        Object.assign(output, { method: 'specificType', args: ['Int32'] });
      } else if (min >= -2147483648 && max <= 2147483647) { Object.assign(output, { method: 'specificType', args: ['Int64'] }); }
    } else if (max <= 255) {
      Object.assign(output, { method: 'specificType', args: ['UInt8'] });
    } else if (max <= 65535) {
      Object.assign(output, { method: 'specificType', args: ['UInt16'] });
    } else if (max <= 16777215) {
      Object.assign(output, { method: 'specificType', args: ['UInt32'] });
    } else {
      Object.assign(output, { method: 'specificType', args: ['UInt64'] });
    }
  } else if (type === 'float') {
    Object.assign(output, { method: 'specificType', args: ['Decimal(19,4)'] });
  } else if (type === 'currency') {
    Object.assign(output, { method: 'specificType', args: ['Decimal(19,2)'] });
  } else if (type === 'date') {
    Object.assign(output, { method: 'specificType', args: ['Date'], defaultValue: '1970-01-01' });
  } else if (type === 'datetime') {
    Object.assign(output, { method: 'specificType', args: ['DateTime'], defaultValue: '1970-01-01 00:00:00' });
  } else if (type === 'string') {
    if (distinct <= 10000) {
      Object.assign(output, { method: 'specificType', args: ['LowCardinality(String)'], defaultValue: '' });
    } else {
      Object.assign(output, { method: 'specificType', args: ['String'], defaultValue: '' });
    }
  } else {
    throw new Error(`Unsupported type:${type}`);
  }

  return output;
};

Worker.prototype.createTable = async function ({
  table: name, columns, indexes = [],
}) {
  if (!columns || columns.length === 0) throw new Error('columns are required to createTable');
  // get the primary key, but that's it
  const pkey = indexes?.find((d) => d.primary);
  const knex = await this.connect();

  const noTypes = columns.filter((c) => {
    if (c.type || c.isKnexDefinition) return false;
    return true;
  });
  if (noTypes.length > 0) throw new Error(`Error creating table ${name}: No type for columns: ${noTypes.map((d) => JSON.stringify(d)).join()}`);

  const colSQL = columns.map((c) => {
    let o = c;
    if (!o.isKnexDefinition) o = this.dialect.standardToKnex(c);
    const {
      args, nullable, defaultValue,
    } = o;
    let s = `${this.escapeColumn(c.name)} ${args[0]}`;

    if (nullable) { // no nullables in ClickHouse!
      // sql+=` NULL`;
    }
    if (defaultValue !== undefined) {
      s += ` DEFAULT ${this.escapeValue(defaultValue)}`;
    }
    return s;
  });
  const sql = `create table ${this.escapeColumn(name)} (${colSQL.join(',')})
      ENGINE=ReplacingMergeTree(${pkey.columns.map((d) => this.escapeColumn(d)).join()})
      ORDER BY ${pkey.columns.map((d) => this.escapeColumn(d)).join()}`;
  await knex.raw(sql);
  return { created: true, table: name };
};
Worker.prototype.createTable.metadata = {
  options: {
    table: {},
    columns: {},
  },
};

/*
Sync data by piping through this process.
Less efficient, but more flexible for different infrastructures
*/
Worker.prototype.syncThroughPipe = async function ({
  table, start, end, dateField,
}) {
  const source = new SQLWorker({ accountId: this.accountId });
  const sourceDesc = await source.describe({ table });
  const indexes = await source.indexes({ table });

  let localDesc = null;
  try {
    localDesc = await this.describe({ table });
  } catch (e) {
    debug(`No such table, creating ${table}`);
  }

  if (!localDesc) {
    const analysis = await source.analyze({ table });
    await this.createTableFromAnalysis({
      table,
      analysis,
      indexes,
    });

    localDesc = await this.describe({ table });
  }
  const conditions = [];

  let sql = `select ${localDesc.columns.map((d) => this.escapeColumn(d.name)).join()} from ${table}`;
  if (start || end) {
    const dateFields = [
      'modified_at',
      'last_modified',
      'created_at',
    ];
    const df = dateField
    || dateFields.find((f) => sourceDesc.fields.find((x) => f === x.name));
    if (!df) throw new Error(`No available date fields in ${sourceDesc.fields.map((d) => d.name)}`);
    if (start) conditions.push(`${this.escapeColumn(df)}>=${this.escapeDate(relativeDate(start))}`);
    if (end) conditions.push(`${this.escapeColumn(df)}<${this.escapeDate(relativeDate(end))}`);
  }
  if (conditions.length > 0) sql += ` WHERE ${conditions.join(' AND ')}`;

  const stream = await source.stream({ sql });

  return this.insertFromStream({ batchSize: 100, table, stream });
};
Worker.prototype.syncThroughPipe.metadata = {
  options: {
    table: {},
    start: {},
    end: {},
  },
};

Worker.prototype.sync = async function ({
  table, start, end, dateField,
}) {
  const source = new SQLWorker({ accountId: this.accountId });
  const sourceDesc = await source.describe({ table });
  const indexes = await source.indexes({ table });

  const conn = process.env.ENGINE9_CLICKHOUSE_SYNC_SOURCE_CONNECTION || source.auth;
  if (!conn) {
    debug('Source keys=', Object.keys(source));
    debug('Source=', source);
    throw new Error('ENGINE9_CLICKHOUSE_SYNC_SOURCE_CONNECTION or source auth is a required environment variable to sync directly from a source');
  }
  debug('Connection string=', conn);
  const {
    host,
    username,
    password,
  } = new URL(conn);

  let localDesc = null;
  try {
    localDesc = await this.describe({ table });
  } catch (e) {
    debug(`No such table, creating ${table}`);
  }
  if (!localDesc) {
    const analysis = await source.analyze({ table });
    await this.createTableFromAnalysis({ table, analysis, indexes });
    localDesc = await this.describe({ table });
  }
  const conditions = [];
  let sql = `insert into ${table} (${localDesc.columns.map((d) => this.escapeColumn(d.name)).join()})
      select ${localDesc.columns.map((d) => this.escapeColumn(d.name)).join()} from mysql(${this.escapeValue(host)}, ${this.escapeValue(this.accountId)}, ${this.escapeValue(table)}, ${this.escapeValue(username)}, ${this.escapeValue(password)})`;
  if (start || end) {
    const dateFields = [
      'modified_at',
      'last_modified',
      'created_at',
    ];
    const df = dateField
    || dateFields.find((f) => sourceDesc.fields.find((x) => f === x.name));
    if (!df) throw new Error(`No available date fields in ${sourceDesc.fields.map((d) => d.name)}`);
    if (start) conditions.push(`${this.escapeColumn(df)}>=${this.escapeDate(relativeDate(start))}`);
    if (end) conditions.push(`${this.escapeColumn(df)}<${this.escapeDate(relativeDate(end))}`);
  }
  if (conditions.length > 0) sql += ` WHERE ${conditions.join(' AND ')}`;

  return this.query({ sql });
};

Worker.prototype.sync.metadata = {
  options: {
    table: {},
    start: {},
    end: {},
  },
};

module.exports = Worker;
