const util = require('node:util');

const Knex = require('knex');
const clickhouse = require('@march_ts/knex-clickhouse-dialect');
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
    client: clickhouse,
    connection: () => connString + this.accountId,
    // optional migrations config
    migrations: {
      directory: 'migrations_clickhouse',
      disableTransactions: true,
      disableMigrationsListValidation: true,
    },
  });
  return this.knex;
};
// This is engine dependent
Worker.prototype.parseQueryResults = function ({ results }) {
  let data; let records; let modified; let columns;
  if (Array.isArray(results)) {
    [data, columns] = results;
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

// Worker.prototype.onDuplicate = function () { return 'on conflict do update set'; };
// Worker.prototype.onDuplicateFieldValue = function (f) { return `excluded.${f}`; };

/*
  Try to determine a viable database structure from an analysis.  This needs to be more specific
  than the normal schema work as it must have specific column details based on ranges,
  whereas not every schema
*/

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

Worker.prototype.sync = async function ({
  table, start, end, dateField,
}) {
  const source = new SQLWorker({ accountId: this.accountId });
  const sourceDesc = await source.describe({ table });

  let localDesc = null;
  try {
    localDesc = await this.describe({ table });
  } catch (e) {
    debug(`No such table, creating ${table}`);
  }
  if (!localDesc) {
    const analysis = await source.analyze({ table });
    await this.createTableFromAnalysis({ table, analysis });
    localDesc = await this.describe({ table });
  }
  const conditions = [];

  let sql = `select * from ${table}`;
  if (start || end) {
    const dateFields = [
      'modified_at',
      'last_modified',
      'created_at',
    ];
    const df = dateField
    || dateFields.find((f) => sourceDesc.fields.find((x) => f === x.name));
    if (!df) throw new Error(`No available date fields in ${sourceDesc.fields.map((d) => d.name)}`);
    if (start) conditions.push(`${this.escapeField(df)}>=${this.escapeDate(relativeDate(start))}`);
    if (end) conditions.push(`${this.escapeField(df)}<${this.escapeDate(relativeDate(end))}`);
  }
  if (conditions.length > 0) sql += ` WHERE ${conditions.join(' AND ')}`;

  const stream = await source.stream({ sql });

  return this.insertFromStream({ table, stream });
};
Worker.prototype.sync.metadata = {
  options: {
    table: {},
    start: {},
    end: {},
  },
};

module.exports = Worker;
