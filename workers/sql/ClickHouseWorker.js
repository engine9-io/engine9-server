/* eslint-disable no-restricted-syntax */
/* eslint-disable no-await-in-loop */
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

Worker.prototype.ensureDatabase = async function ensureDatabase() {
  if (this.knex) return this.knex;
  if (!this.accountId) throw new Error('No accountId specified');
  if (!/^[a-z0-9_]+$/.test(this.accountId)) throw new Error(`Invalid accountId:${this.accountId}`);

  const connString = process.env.ENGINE9_CLICKHOUSE_CONNECTION;
  if (!connString) {
    debug(Object.keys(process.env));
    throw new Error('ENGINE9_CLICKHOUSE_CONNECTION is a required environment variable');
  }
  const dbKnex = new Knex({
    client: 'mysql2', // use mysql protocol, as the only existing one has flaws with insert...select
    connection: () => connString,
  });

  await dbKnex.raw(`create database if not exists ${this.accountId}`);
  await dbKnex.destroy();
  return { database: this.accountId };
};
Worker.prototype.ensureDatabase.metadata = {
  options: {},
};

/*
  Try to determine a viable database structure from an analysis.  This needs to be more specific
  than the normal schema work as it must have specific column details based on ranges,
  whereas not every schema item does
*/

const namedTypes = {
  given_name: { method: 'specificType', args: ['String'], defaultValue: '' },
  family_name: { method: 'specificType', args: ['String'], defaultValue: '' },
  email_hash_v1: { method: 'specificType', args: ['String'], defaultValue: '' },
  phone_hash_v1: { method: 'specificType', args: ['String'], defaultValue: '' },
  amount: { method: 'specificType', args: ['Decimal(19,2)'], defaultValue: 0 },
  refund_amount: { method: 'specificType', args: ['Decimal(19,2)'], defaultValue: 0 },
  transaction_bot_id: { method: 'specificType', args: ['LowCardinality(String)'], defaultValue: 0 },
  remote_transaction_id: { method: 'specificType', args: ['String'], defaultValue: 0 },
  source_code: { method: 'specificType', args: ['String'], defaultValue: '' },
  final_primary_source_code: { method: 'specificType', args: ['String'], defaultValue: '' },
};
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
  if (namedTypes[name]) {
    return Object.assign(output, namedTypes[name]);
  }
  if (type === 'int') {
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
      } else if (min >= -2147483648 && max <= 2147483647) {
        Object.assign(output, { method: 'specificType', args: ['Int32'] });
      } else {
        Object.assign(output, { method: 'specificType', args: ['Int64'] });
      }
    } else if (max <= 255) {
      Object.assign(output, { method: 'specificType', args: ['UInt8'] });
    } else if (max <= 65535) {
      Object.assign(output, { method: 'specificType', args: ['UInt16'] });
    } else if (max <= 4294967295) {
      Object.assign(output, { method: 'specificType', args: ['UInt32'] });
    } else {
      Object.assign(output, { method: 'specificType', args: ['UInt64'] });
    }
  } else if (type === 'double') {
    Object.assign(output, { method: 'specificType', args: ['Decimal(19,4)'] });
  } else if (type === 'decimal') {
    Object.assign(output, { method: 'specificType', args: ['Decimal(19,2)'] });
  } else if (type === 'currency') {
    Object.assign(output, { method: 'specificType', args: ['Decimal(19,2)'] });
  } else if (type === 'uuid') {
    Object.assign(output, { method: 'specificType', args: ['UUID'], defaultValue: '00000000-0000-0000-0000-000000000000' });
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
  table: name, columns, indexes = [], primary,
}) {
  if (!columns || columns.length === 0) throw new Error('columns are required to createTable');
  // get the primary key, but that's it
  let pkey = primary;
  if (pkey) pkey = { columns: [primary] };
  if (!primary)pkey = indexes?.find((d) => d.primary);
  if (!pkey) pkey = indexes?.find((d) => d.unique);
  if (!pkey) {
    debug('indexes:', JSON.stringify(indexes));
    debug('columns', columns.map((d) => d.name).join());
    throw new Error(`Error creating table ${name}, ClickHouse requires a primary or unique key, none were found in existing indexes or in a primary option`);
  }
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
    const type = args[0];
    if (type === undefined) {
      debug(c);
      throw new Error(`Invalid definition for column ${c.name} on table ${name}`);
    }
    let s = `${this.escapeColumn(c.name)} ${type}`;

    if (nullable) { // no nullables in ClickHouse!
      // sql+=` NULL`;
    }

    if (defaultValue !== undefined) {
      if (defaultValue === null) {
        if (type === 'String') {
          s += ' DEFAULT \'\'';// no nulls
        } else {
          s += ' DEFAULT 0';// no nulls
        }
      } else {
        s += ` DEFAULT ${this.escapeValue(defaultValue)}`;
      }
    }
    return s;
  });
  let ENGINE = 'ENGINE=MergeTree()';
  if (pkey) {
    ENGINE = 'ENGINE=ReplacingMergeTree()';
  }
  const sql = `create table ${this.escapeColumn(name)} (${colSQL.join(',')})
      ${ENGINE}
      ORDER BY (${pkey.columns.map((d) => this.escapeColumn(d)).join()})`;
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
  await this.ensureDatabase();
  const source = new SQLWorker({ accountId: this.accountId });
  const sourceDesc = await source.describe({ table });
  const indexes = await source.indexes({ table });

  let conn = process.env.ENGINE9_CLICKHOUSE_SYNC_SOURCE_CONNECTION
     || source.auth?.database_connection;
  if (!conn) {
    debug('Source keys=', Object.keys(source));
    debug('Source=', source);
    throw new Error('ENGINE9_CLICKHOUSE_SYNC_SOURCE_CONNECTION or source auth is a required environment variable to sync directly from a source');
  }
  // temporarily pull from the replica
  conn = conn.replace('warehouse.frakture.com', '10.211.2.169');
  debug('Connection string=', conn);
  const {
    host,
    username,
    password,
    pathname,
    includeData = true,
  } = new URL(conn);

  let localDesc = null;
  try {
    localDesc = await this.describe({ table });
  } catch (e) {
    debug(`No such table, creating ${table}`);
  }
  if (!localDesc) {
    const analysis = await source.analyze({ table });
    if (analysis.records < 100) {
      await this.createTable({ table, indexes, ...sourceDesc });
    } else {
      try {
        await this.createTableFromAnalysis({ table, analysis, indexes });
      } catch (e) {
        debug('analysis=', analysis);
        debug(`Error creating table ${table}`);
        throw e;
      }
    }
    localDesc = await this.describe({ table });
  }
  if (!includeData) return { table, noData: true };
  const conditions = [];
  let sql = `insert into ${table} (${localDesc.columns.map((d) => this.escapeColumn(d.name)).join()})
      select ${localDesc.columns.map((d) => this.escapeColumn(d.name)).join()} from mysql(${this.escapeValue(host)}, ${this.escapeValue(pathname?.slice(1) || this.accountId)}, ${this.escapeValue(table)}, ${this.escapeValue(username)}, ${this.escapeValue(unescape(password))})`;
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

Worker.prototype.syncAll = async function ({
  tables, filter, exclude,
}) {
  await this.ensureDatabase();
  const source = new SQLWorker({ accountId: this.accountId });

  const { tables: tableNames } = await source.tables({
    type: 'table', tables, filter, exclude,
  });
  for (const table of tableNames) {
    debug(`Syncing ${table}`);
    await this.drop({ table });
    await this.sync({ table });
  }

  const { tables: viewNames } = await source.tables({
    type: 'view', tables, filter, exclude,
  });
  // Create intermediate views so they can be out of order
  // but ignore missing ones
  const views = {
    success: [],
    fail: [],
  };
  for (const table of viewNames) {
    debug(`Pre-preprocessing view ${table}`);
    try {
      // set up a temporary TABLE, which has all the correct types
      // it's not easy to create a view with correct typing
      const { columns } = await source.describe({ table });
      await this.drop({ table });
      await this.createTable({ table, columns });
      views.success.push(table);
    } catch (e) {
      debug(e);
      views.fail.push(table);
    }
  }
  for (const table of views.success) {
    debug(`Processing view ${table}`);
    const { sql } = await source.getCreateView({ table });
    const parts = sql.split(/\sFROM\s/ig);
    const sqlClean = [].concat(parts[0]).concat(parts.slice(1).map((d) => d.replace(/[()]/g, ''))).join('\nFROM\n');

    await this.drop({ table });
    await this.query({ sql: sqlClean });
  }
  return { tableNames, views };
};
Worker.prototype.syncAll.metadata = {
  options: {
    filter: {},
    start: {},
    end: {},
  },
};

Worker.prototype.sizes = async function () {
  return this.query(`select parts.*,
       columns.compress_size,
       columns.uncompress_size,
       columns.compress_ratio,
       columns.compress_percent
from (
         select database,table,
                formatReadableSize(sum(data_uncompressed_bytes))          AS uncompress_size,
                formatReadableSize(sum(data_compressed_bytes))            AS compress_size,
                round(sum(data_compressed_bytes) / sum(data_uncompressed_bytes), 3) AS  compress_ratio,
                round((100 - (sum(data_compressed_bytes) * 100) / sum(data_uncompressed_bytes)), 3) AS compress_percent

             from system.columns
             group by database,table
         ) columns
         right join (
    select database,
            table,
           sum(rows)                                            as rows,
           max(modification_time)                               as latest_modification,
           formatReadableSize(sum(bytes))                       as disk_size,
           formatReadableSize(sum(primary_key_bytes_in_memory)) as primary_keys_size,
           any(engine)                                          as engine,
           sum(bytes)                                           as bytes_size
    from system.parts
    where active and database=${this.escapeValue(this.accountId)}
    group by database, table
    ) parts on columns.database=parts.database and columns.table = parts.table
order by parts.bytes_size desc`);
};
Worker.prototype.sizes.metadata = {
  options: {},
};

module.exports = Worker;
