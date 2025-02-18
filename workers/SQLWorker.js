/* eslint-disable camelcase */
const util = require('util');
const info = require('debug')('info:SQLWorker');
const debug = require('debug')('SQLWorker');
const debugMore = require('debug')('debug:SQLWorker');

const Knex = require('knex');
const { getUUIDv7 } = require('@engine9/packet-tools');
const { Readable } = require('stream');
const through2 = require('through2');
const JSON5 = require('json5');
const config = require('../account-config.json');
const {
  ObjectError,
  bool, toCharCodes, parseRegExp, parseJSON5,
} = require('../utilities');
const analyzeStream = require('../utilities/analyze');
const mysqlDialect = require('./sql/dialects/MySQL');

const BaseWorker = require('./BaseWorker');
const FileWorker = require('./FileWorker');
const CommandLine = require('./sql/CommandLine');

require('dotenv').config({ path: '.env' });

function Worker(worker) {
  BaseWorker.call(this, worker);
  this.accountId = worker.accountId;
  if (!this.accountId) throw new Error('No accountId provided to SQLWorker constructor');
  if (worker.knex) {
    this.knex = worker.knex;
  } else if (worker.auth) {
    this.auth = {
      ...worker.auth,
    };
  } else {
    debug(`Getting auth from account-config.json for account ${worker.accountId}`);
    this.auth = config.accounts?.[worker.accountId]?.auth;
    if (!this.auth) throw new Error(`No authorization available for accountId:${worker.accountId}`);
  }
  this.dialect = mysqlDialect;
}

util.inherits(Worker, BaseWorker);
// eslint-disable-next-line no-restricted-syntax,guard-for-in
for (const i in CommandLine.prototype) { Worker.prototype[i] = CommandLine.prototype[i]; }
Worker.metadata = {
  alias: 'sql',
};

Worker.defaultStandardColumn = {
  name: '',
  type: '',
  length: null,
  nullable: true,
  default_value: undefined, // null would actually means something here
  auto_increment: false,
};

Worker.prototype.info = function () {
  return {
    driver: 'mysql',
    dialect: this.dialect,
  };
};

Worker.prototype.connect = async function connect() {
  if (this.knex) {
    if (!this.version) {
      const [[{ version }]] = await this.knex.raw('select version() as version');
      this.version = version;
    }
    return this.knex;
  }
  const { accountId } = this;
  if (!accountId) throw new Error('accountId is required for connect method');

  let authConfig = null;
  const s = this.auth.database_connection;
  if (!s) throw new Error(`SQLWorker Could not find database_connection settings in auth configuration for account ${accountId} with keys: ${Object.keys(this.auth)}`);
  if (s.match(/[#{}]+/)) throw new Error('Invalid connection string, contains some unescaped characters');

  authConfig = {
    client: 'mysql2',
    connection: s,
  };
  debug(`***** new Knex instance for account ${accountId}`);
  this.knex = Knex(authConfig);
  const [[{ version }]] = await this.knex.raw('select version() as version');
  this.version = version;
  return this.knex;
};
Worker.prototype.ok = async function f() {
  const knex = await this.connect();
  const [[data]] = await knex.raw('select 1 as ok');
  return data;
};
Worker.prototype.ok.metadata = {
  options: {},
};

Worker.prototype.databases = async function f() {
  return this.query('show databases');
};
Worker.prototype.databases.metadata = {
  options: {},
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
// values can be undefined or null,
// implying we don't want to use any bindings -- may have already been bound
Worker.prototype.query = async function (options) {
  let opts = options;
  if (typeof options === 'string') {
    opts = { sql: options };
  }
  if (!opts.sql) throw new Error('No sql provided');
  try {
    const knex = await this.connect();
    this.debugCounter = (this.debugCounter || 0) + 1;
    if (this.debugCounter < 10) debug(`Running:${this.debugCounter}`, `${opts.sql.slice(0, 500)}...`);
    const results = await knex.raw(opts.sql, opts.values || []);

    return this.parseQueryResults({ sql: opts.sql, results });
  } catch (e) {
    info('Error running query:', this.accountId, this.info(), options, e);
    throw e;
  }
};
Worker.prototype.query.metadata = {
  options: {
    sql: {},
    values: { description: 'Array of escapable values' },
  },
};

Worker.prototype.tables = async function f(options = {}) {
  let sql = 'select TABLE_NAME from information_schema.tables where table_schema=';
  const values = [];
  if (options.database) {
    sql += '?';
    values.push(options.database);
  } else {
    sql += 'database()';
  }
  if (options.type === 'view') {
    sql += " and table_type='VIEW'";
  } else if (options.type === 'table') {
    sql += " and table_type='BASE TABLE'";
  }

  let d = await this.query({ sql, values });
  d = d.data.map((t) => t.TABLE_NAME || t.table_name);
  d.sort((a, b) => (a < b ? -1 : 1));

  if (options.tables) {
    const tables = options.tables.split(',').map((t) => t.trim().toLowerCase()).filter(Boolean);
    d = d.filter((t) => tables.indexOf(t.toLowerCase()) >= 0);
  }

  if (options.filter) {
    const filters = options.filter.split(',').map((r) => parseRegExp(r));
    d = d.filter((t) => filters.some((r) => t.match(r)));
  }

  if (options.exclude) {
    const exclude = options.exclude.split(',').map((x) => x.trim().toLowerCase());
    d = d.filter((t) => exclude.indexOf(t.toLowerCase()) < 0);
  }
  if (bool(options.exclude_temp_tables, false)) { d = d.filter((t) => t.indexOf('temp_') !== 0); }

  return {
    tables: d,
    records: d.length,
  };
};

Worker.prototype.tables.metadata = {

  options: {
    tables: { description: 'Comma delimited list of tables to include' },
    filter: { description: 'Comma delimited regular expressions to match' },
    exclude: { description: 'Exclude tables that are included in this comma delimited list' },
    type: { description: 'Type of table to show: view, table, or both(default)' },
    exclude_temp_tables: { description: 'Exclude tables that are prefixed with temp_ (default false)' },
  },
};

Worker.prototype.tables.metadata = {
  options: {},
};

const columnNameMatch = /^[a-zA-Z0-9_]+$/;
Worker.prototype.escapeColumn = function (f) {
  // Engine 9 follows a very restrictive column name standard, intended for cross-compatability
  // Thus, escaping column names is also about validating they're following the rules

  if (!f.match(columnNameMatch)) throw new Error(`Invalid field name: ${f}`);

  return this.dialect.escapeColumn(f);
};
Worker.prototype.escapeValue = function (t) {
  return this.dialect.escapeValue(t);
};

Worker.prototype.escapeDate = function (d) {
  if (d && typeof d.toISOString === 'function') {
    const s = d.toISOString();
    if (!s) return this.escapeValue('1900-01-01'); // This is an invalid date, but to try to maximize compatibility set to 1900
    return this.escapeValue(s.slice(0, -1));
  }
  if (d && typeof d === 'string' && d.slice(-1)[0] === 'Z') return this.escapeValue(d.slice(0, -1));
  return this.escapeValue(d);
};

Worker.prototype.addLimit = function (sql, limit, offset) {
  return this.dialect.addLimit(sql, limit, offset);
};

const tableNameMatch = /^[a-zA-Z0-9_]+$/;
Worker.prototype.escapeTable = function escapeTable(t) {
  if (!t.match(tableNameMatch)) throw new Error(`Invalid table name: ${t}`);
  return t;
};

Worker.prototype.indexes = async function indexes({ table, unique, primary }) {
  let sql = `SELECT index_name,group_concat(column_name order by seq_in_index) as columns, not(non_unique) as \`unique\` 
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

  return d[0].map((i) => ({
    index_name: i.INDEX_NAME || i.index_name,
    columns: i.columns.split(','),
    primary: (i.INDEX_NAME || i.index_name) === 'PRIMARY',
    unique: !!i.unique,
  }));
};

Worker.prototype.indexes.metadata = {
  options: {
    table: { required: true },
    unique: { description: 'Only include the unique indexes?  Default no' },
    primary: { description: 'Only include the primary index?  Default no' },
  },
};

Worker.prototype.describe = async function describe(opts) {
  const { table, raw } = opts;
  if (!table) throw new Error(`No table provided to describe with opts ${Object.keys(opts)}`);
  if (table === 'dual') return { database: 'dual', columns: [] };// special handling for function calls
  debugMore(`Describing ${table}`, { accountId: this.accountId });
  const sql = `select database() as DB,COLUMN_NAME,COLUMN_TYPE,DATA_TYPE,IS_NULLABLE,COLUMN_DEFAULT,CHARACTER_MAXIMUM_LENGTH,EXTRA FROM information_schema.columns WHERE  table_schema = Database() AND table_name = '${this.escapeTable(table)}' order by ORDINAL_POSITION`;
  const r = await this.query(sql);
  const cols = r.data;
  if (cols.length === 0) throw new ObjectError({ message: `Could not find table ${table}`, code: 'DOES_NOT_EXIST' });
  // databases return back arbitrary capitalization from information_schema
  cols.forEach((c) => { Object.keys(c).forEach((k) => { c[k.toUpperCase()] = c[k]; }); });

  const results = {};
  results.database = cols[0].DB;
  results.columns = cols.map((d) => {
    let defaultValue = d.COLUMN_DEFAULT;
    if (defaultValue === 'NULL') {
      defaultValue = null;
    } else if (defaultValue === null) {
      // in our world this is undefined
      defaultValue = undefined;
    } else {
      const type = d.COLUMN_TYPE.toUpperCase();
      if (type === 'UUID') {
        if (defaultValue.indexOf("'") === 0) defaultValue = defaultValue.slice(1, -1);
      } else if (type === 'TINYINT(1)') {
        defaultValue = defaultValue === '1';
      } else if (type.indexOf('INT') === 0
      || type.indexOf('BIGINT') === 0) {
        defaultValue = parseInt(defaultValue, 10);
      } else if (type.indexOf('FLOAT') === 0
            || type.indexOf('DOUBLE') === 0
            || type.indexOf('DECIMAL') === 0
      ) {
        defaultValue = parseFloat(defaultValue, 10);
      }
    }

    const o = {
    // raw: d,
      name: d.COLUMN_NAME,
      column_type: d.COLUMN_TYPE,
      length: d.CHARACTER_MAXIMUM_LENGTH,
      nullable: d.IS_NULLABLE.toUpperCase() === 'YES',
      extra: d.EXTRA, // not standardized, but still useful
      default_value: defaultValue,
      auto_increment: (d.EXTRA || '').toUpperCase().indexOf('AUTO_INCREMENT') >= 0,
    };
    if (bool(raw)) {
      return o;
    }
    try {
      return this.dialect.dialectToStandard(o, {} || Worker.defaultColumn);
    } catch (e) {
      info(`Error while describing table ${table}`);
      throw e;
    }
  });
  debugMore(`Finished describing ${table}`);
  return results;
};

Worker.prototype.describe.metadata = {
  options: {
    table: { required: true },
    raw: {
      description: "Don't try to convert to standard, default false",
    },
  },
};

Worker.prototype.getTypeQuery = function (table) {
  return `SELECT TABLE_TYPE as type FROM information_schema.tables where table_schema = Database() AND table_name = ${this.escapeValue(table)}`;
};

Worker.prototype.tableType = async function (options) {
  const sql = this.getTypeQuery(options.table);
  const { data } = await this.query(sql);

  if (!data[0]) {
    debug({ sql, data });
    const error = new Error(`tableType: Could not find type of table ${options.table}`);
    error.does_not_exist = true;
    throw error;
  }

  let t = null;
  const test = data[0].type || data[0].TYPE;

  switch (test) {
    case 'BASE TABLE':
    case 'TABLE':
      t = 'table'; break;
    case 'VIEW':
      t = 'view'; break;
    default:
      throw new Error(`tableType does not recognize type ${test}`);
  }

  return t;
};

Worker.prototype.tableType.metadata = {
  options: {
    table: { required: true },
  },
};

Worker.prototype.stream = async function ({ sql }) {
  if (!sql) throw new Error('stream required a sql parameter');
  const knex = await this.connect();
  return new Promise((resolve, reject) => {
    debug(`Streaming ${sql.slice(0, 500)}`);
    const stream = knex.raw(sql).stream();
    let err = null;
    stream.on('error', (e) => {
      debug(e);
      err = e;
    });
    setTimeout(() => {
      if (err) reject(err);
      else resolve(stream);
    }, 100);
  });
};

Worker.prototype.stream.metadata = {
  options: {
    sql: { required: true },
  },
};

Worker.prototype.insertFromQuery = async function (options) {
  const knex = await this.connect();
  const stream = await knex.raw(options.sql).stream();
  return this.insertFromStream({ ...options, stream });
};

Worker.prototype.insertFromQuery.metadata = {
  options: {
    sql: { required: true },
    table: { required: true },
    upsert: {},
  },
};

/*
  Loads a table from a query by
    a) Getting field names by running the sql with 0 records
    b) using the field names to pick the target field names
    c) running an insert <sql>
*/

Worker.prototype.loadTableFromQuery = async function (options) {
  const { table } = options;
  if (!table) throw new Error('table required');
  const unlimitedSQL = options.sql.replace(/limit\s*[0-9]+/, '');
  const { columns } = await this.query(`${unlimitedSQL} limit 0`);

  return this.query(`insert into ${table} (${columns.map((d) => d.name).join(',')}) ${options.sql}`);
};

Worker.prototype.loadTableFromQuery.metadata = {
  options: {
    sql: { required: true },
    table: { required: true },
  },
};

Worker.prototype.deduceColumnDefinition = function (o) {
  if (o.isKnexDefinition) return o;
  return this.dialect.standardToKnex(o, this.version);
};

Worker.prototype.createTableFromAnalysis = async function ({
  table, analysis, indexes, primary, initialColumns = [],
}) {
  if (!analysis) throw new Error('analysis is required');
  await this.connect();// set variables, etc
  const columns = initialColumns.concat(
    analysis.fields.map((f) => {
      let { name } = f;
      name = name.toLowerCase().replace(/[^a-z0-9_]/g, '_');
      return {
        isKnexDefinition: true,
        name,
        ...this.deduceColumnDefinition(f, this.version),
      };
    }),
  );
  debug(columns);
  return this.createTable({
    table, columns, indexes, primary,
  });
};

Worker.prototype.createTableFromAnalysis.metadata = {
};

Worker.prototype.createAndLoadTable = async function (options) {
  try {
    const fworker = new FileWorker(this);
    const stream = await fworker.fileToObjectStream(options);
    const analysis = await analyzeStream(stream);
    const table = options.table || `temp_${new Date().toISOString().replace(/[^0-9]/g, '_')}`.slice(0, -1);
    const indexes = options.indexes || [];
    if (options.primary) {
      indexes.push({
        primary: true,
        columns: options.primary.split(','),
      });
    }

    await this.createTableFromAnalysis({
      table,
      analysis,
      initialColumns: options.initialColumns,
    });
    const { columns } = await this.describe({ table });
    const stream2 = await fworker.fileToObjectStream({
      ...options,
      columns: columns.map((d) => d.name),
    });

    const streamResults = await this.insertFromStream({
      ...options,
      table,
      stream: stream2.stream,
    });
    return { ...streamResults, columns };
  } catch (e) {
    debug(`Error creating and loading table with options:${JSON.stringify(options, null, 4)}`);
    throw e;
  }
};
Worker.prototype.createAndLoadTable.metadata = {
  options: {
    table: {},
    filename: {},
    stream: {},
    indexes: {},
    primary: { description: 'In addition to indexes, specify a string primary key' },
  },
};

Worker.prototype.insertOne = async function ({ table, data }) {
  const knex = await this.connect();
  return knex.table(table).insert(data);
};

Worker.prototype.upsertMessage = async function (opts) {
  if (opts.message_id !== undefined) throw new Error('upsert message should have an id, not a message_id');
  const knexTransaction = await this.connect();
  return knexTransaction.transaction(async (knex) => {
    const desc = await this.describe({ table: 'message' });
    const message = desc.columns.reduce((a, b) => {
      if (opts[b.name] !== undefined)a[b.name] = opts[b.name];
      return a;
    }, {});
    let message_id = message.id;
    if (!message_id) {
      message_id = getUUIDv7(opts.publish_date || undefined);
      message.id = message_id;
      await knex.table('message').insert(message);
    } else {
      await knex.table('message').where({ id: message_id }).update(message);
    }
    debug(`Finished upserting message id ${message_id}`);
    const descContent = await this.describe({ table: 'message_content' });
    const messageContent = descContent.columns.reduce((a, b) => {
      if (b.name === 'id') return a;// don't assign ids
      if (opts[b.name] !== undefined)a[b.name] = opts[b.name];
      return a;
    }, {});
    if (Object.keys(messageContent).length === 0) return { id: message_id };
    const existingMessageContentRecord = await knex.table('message_content')
      .select('id')
      .where({ message_id });
    if (messageContent.content) {
      messageContent.content = JSON.stringify(parseJSON5(messageContent.content));
    }
    if (messageContent.remote_data) {
      messageContent.remote_data = JSON.stringify(parseJSON5(messageContent.remote_data));
    }
    if (existingMessageContentRecord.length === 0) {
      messageContent.message_id = message_id;
      await knex.table('message_content').insert(messageContent);
    } else {
      await knex.table('message_content').where({ id: existingMessageContentRecord[0].id }).update(messageContent);
    }

    return { id: message_id };
  });
};
Worker.prototype.upsertMessage.metadata = {
  options: {
    id: {},
    name: {},
    channel: {},
    subject: {},
    content: {},
    remote_data: {},
  },
};

Worker.prototype.updateOne = async function ({
  table, id, data,
}) {
  if (!id) throw new Error('No id to update');
  const knex = await this.connect();
  await knex.table(table).where({ id }).update(data);
  return { id };
};

Worker.prototype.appendLimit = function (_query, optLimit, _offset) {
  let offset = _offset;
  let query = _query.trim();
  let limit = null;
  // extract existing limits from the query
  let a = query.match(/limit\s*([0-9]*)\s*,\s*([0-9])*/i);
  if (a) {
    query = query.replace(/limit\s*[0-9,\s]*/i, '');
    [, offset, limit] = a;
  } else {
    a = query.match(/limit\s*([0-9]*)\s*offset\s*([0-9])*/i) || query.match(/limit\s*([0-9]*)/i);
    if (a) {
      query = query.replace(/limit\s*[0-9,\s]*/i, '');
      [, limit, offset] = a;
    } else {
      // sqlserver format
      a = query.match(/select\s*top\s*([0-9])\s/i);
      if (a) {
        query = query.replace(/select\s*top\s*([0-9]*)\s/i, 'select ');
        [, limit] = a;
      }
    }
  }

  // If there wasn't one ANYWHERE, just return the query as is
  if (limit == null && (optLimit === undefined)) {
    return query;
  }

  // if the limit is specified -- could be 0, use that
  if (optLimit !== undefined) limit = optLimit;

  // otherwise use what was in the query
  query = this.addLimit(query, limit, offset);
  return query;
};

Worker.prototype.stringToType = function (_v, _t, length, nullable, nullAsString) {
  const worker = this;
  let t = _t; let v = _v;

  t = t.toLowerCase();
  let dt = null;
  switch (t) {
    case 'date':
      if (v === null && nullable) return null;
      dt = new Date(v);
      if (dt === 'Invalid Date') return null;
      return dt.toISOString().slice(0, 10);

    case 'time':
    case 'datetime':
    case 'datetime2':
    case 'datetimeoffset':
    case 'smalldatetime':
    case 'timestamp':
    case 'timestamp_ntz':
    case 'timestamp without time zone':
      if (v === null && nullable) return null;
      // this is commented because an undefined date or time is usually a bug on the input
      // if (v === undefined && nullable) return null;
      dt = new Date(v);
      if (dt === 'Invalid Date') return null;
      return dt.toISOString().slice(0, -5);

    case 'bit':
    case 'int':
    case 'integer':
    case 'bigint':
    case 'smallint':
      if (v === 0) {
        // we're good
        break;
      }
      if (v === '' || v === undefined || v === 'NULL') {
        if (!nullable) v = 0;
        else v = null;
      }
      if (typeof v === 'string') v = v.replace(/[,$]/g, '');
      if (v === parseFloat(v)) v = parseFloat(v);

      break;
    case 'tinyint':
      // blank or undefined is null, or 0 if not nullable
      if (v === '' || v === undefined || v === 'NULL') {
        if (!nullable) v = 0;
        else v = null;
      }

      if (typeof v === 'string') {
        v = v.replace(/[,$]/g, '');
      }
      // for tinyint, it could be a number, so try that first
      if (v === parseFloat(v)) v = parseFloat(v);
      // otherwise it could be a boolean value
      else if (v) {
        v = bool(v);
        // tinyint supports 0 & 1
        v = v ? 1 : 0;
      }

      break;
    case 'text':
    case 'mediumtext':
    case 'enum':
    case 'ntext':
    case 'char':
    case 'varchar':
    case 'nvarchar':
    case 'nvarchar2':
    case 'varchar2':
    case 'character varying': // this is for PostgreSQL
      if (v === '' || v === undefined || (v === 'NULL' && !nullAsString)) {
        if (!nullable) { v = ''; } else v = null;
      } else if (v && length) {
        if (typeof v !== 'string') v = JSON.stringify(v);
        if (v.length > length) {
          if (worker.do_not_slice) {
            /*
            This is typically used because the Node.js, and MySQL length() functions
            use a different length for unicode than the column definition,
            which uses the char_length() variety.
            */
          } else if (worker.error_on_slice) {
            throw new Error(`Value too long, should be ${length} characters but is ${v.length}:${v}`);
          } else {
            v = v.slice(0, length);
          }
        }
      }
      break;
    case 'decimal':
    case 'float':
    case 'money':
    case 'numeric':
    case 'smallmoney':
    case 'real':
    case 'double':
      if (v === 0) return 0;
      if (v === '' || v === undefined || v === 'NULL') {
        if (!nullable) return 0;
        return null;
      }
      if (typeof v === 'string') v = v.replace(/[,$]/g, '');
      v = parseFloat(v) || 0;
      break;
    case 'jsonb':
      v = typeof v === 'object' ? JSON.stringify(v) : v;
      break;
    default:
  }
  return v;
};

Worker.prototype.getSQLName = function (n) {
  return n.trim().replace(/[^0-9a-zA-Z_-]/g, '_').toLowerCase();
};

Worker.prototype.getCreateView = async function (options) {
  const { table } = options;
  const worker = this;
  const { data } = await worker.query({ sql: `show create view ${this.escapeTable(table)}` });
  let sql = data[0]['Create View'];

  if (!sql) throw new Error(`Could not find 'Create View' in ${Object.keys(data[0])}`);
  sql = sql.replace(/ALGORITHM=UNDEFINED DEFINER=[^\s]* SQL SECURITY DEFINER /, '');

  return { sql };
};
Worker.prototype.getCreateView.metadata = {
  bot: true,
  options: { table: {} },
};

Worker.prototype.createView = async function (options) {
  let sql = options.sql || await this.buildSqlFromEQLObject(options);
  if (bool(options.replace, false)) {
    sql = `CREATE VIEW ${options.name} AS ${sql}`;
  } else {
    sql = `CREATE OR REPLACE VIEW ${options.name} AS ${sql}`;
  }

  this.query(sql);
};

Worker.prototype.createTable = async function ({
  table: name, columns, timestamps = false, indexes = [],
}) {
  if (!columns || columns.length === 0) throw new Error('columns are required to createTable');
  const knex = await this.connect();
  const [[{ version }]] = await knex.raw('select version() as version');

  await knex.schema.createTable(name, (table) => {
    const noTypes = columns.filter((c) => {
      if (c.type || c.isKnexDefinition) return false;
      return true;
    });
    if (noTypes.length > 0) throw new Error(`Error creating table ${name}: No type for columns: ${noTypes.map((d) => JSON.stringify(d)).join()}`);
    const autoIncrements = [];

    columns.forEach((c) => {
      let o = c;
      try {
        if (!o.isKnexDefinition) o = this.dialect.standardToKnex(c, version);
      } catch (e) {
        if (e) {
          debug(`Error with column ${c}`);
          debug(e);
          throw e;
        }
      }
      const {
        method, args, nullable, unsigned, defaultValue, defaultRaw,
      } = o;

      if (method === 'increments' || method === 'bigIncrements') {
        autoIncrements.push(o);
      }

      const m = table[method].apply(table, [c.name, ...args]);
      if (unsigned) m.unsigned();
      if (nullable) {
        m.nullable();
      } else {
        m.notNullable();
      }
      if (defaultRaw !== undefined) {
        const allowedRaw = ['current_timestamp()',
          'current_timestamp() on update current_timestamp()'];
        if (allowedRaw.indexOf(defaultRaw) < 0) throw new Error(`createTable: Invalid knex raw value:'${defaultRaw}'`);
        m.defaultTo(knex.raw(defaultRaw));
      } else if (defaultValue !== undefined) {
        if (defaultValue === null && !nullable) {
          throw new Error(`Error with column definition for ${c.name}, the default is null, but the column isn't nullable.  Use undefined for default processing`);
        }
        m.defaultTo(defaultValue);
      }
    });

    const primaries = columns.filter((d) => d.primary_key).map((c) => c.name);

    if (primaries.length > 0) {
      table.primary(primaries);
    }
    indexes.forEach((x) => {
      const indexName = getUUIDv7();
      if (x.primary) {
        if (primaries.length > 0) {
          throw new Error(`Should not specify both a primary key as a column (${primaries}) and in an index (${x})`);
        } else if (autoIncrements.length > 0) {
          // do nothing - this is a knex weirdness
          // that clobbers the auto_increments if the primary key is reset
        } else {
          table.primary(x.columns, indexName);
        }
      } else if (x.unique) {
        table.unique(x.columns, indexName);
      } else {
        table.index(x.columns, indexName);
      }
    });

    if (timestamps) table.timestamps();
  });
  return { created: true, table: name };
};
Worker.prototype.createTable.metadata = {
  options: {
    table: {},
    columns: {},
  },
};

Worker.prototype.alterTable = async function ({ table: name, columns = [], indexes = [] }) {
  const knex = await this.connect();
  const [[{ version }]] = await knex.raw('select version() as version');
  await knex.schema.alterTable(name, (table) => {
    const noTypes = columns.filter((c) => !c.type);
    if (noTypes.length > 0) throw new Error(`Error altering table ${name} No type for columns: ${columns.map((d) => d.name).join()}`);

    columns.forEach((c) => {
      const {
        method, args, nullable, unsigned, defaultValue, defaultRaw,
      } = this.dialect.standardToKnex(c, version);
      debug(`Altering column ${c.name}`, c, 'Knex opts=', {
        method, args, nullable, unsigned, defaultValue, defaultRaw,
      });
      const column = table[method].apply(table, [c.name, ...args]);
      if (unsigned) column.unsigned();
      if (nullable) {
        column.nullable();
      } else {
        column.notNullable();
      }
      if (defaultRaw !== undefined) {
        const allowedRaw = ['current_timestamp()',
          'current_timestamp() on update current_timestamp()'];
        if (allowedRaw.indexOf(defaultRaw.toLowerCase()) < 0) throw new Error(`alterTable: Invalid knex raw value:'${defaultRaw}'`);
        column.defaultTo(knex.raw(defaultRaw));
      } else if (defaultValue !== undefined) {
        column.defaultTo(defaultValue);
      }
      if (c.differences === 'new') {
        // knex by default adds it in
      } else {
        column.alter();
      }
    });
    const primaries = columns.filter((d) => d.primary_key).map((c) => c.name);
    if (primaries.length > 0) table.primary(primaries);
    indexes.forEach((x) => {
      if (x.primary) {
        if (primaries.length > 0) {
          throw new Error(`Should not specify both a primary key as a column (${primaries}) and in an index (${x})`);
        } else {
          table.primary(x.columns);
        }
      } else if (x.unique) {
        table.unique(x.columns);
      } else {
        table.index(x.columns);
      }
    });
  });
  return { altered: true, table: name };
};
Worker.prototype.alterTable.metadata = {
  options: {
    table: {},
    columns: {},
    indexes: {},
  },
};

Worker.prototype.onDuplicate = function () { return 'on duplicate key update'; };
Worker.prototype.onDuplicateFieldValue = function (f) { return `VALUES(${f})`; };

Worker.prototype.buildInsertSql = function (options) {
  const worker = this;
  const {
    knex, table, columns, rows, upsert = false, ignoreDupes = false, returning = [],
  } = options;

  if (columns.length === 0) throw new Error(`no columns provided for table ${table} before createInsert was called`);
  let sql = 'INSERT';
  if (bool(ignoreDupes, false)) {
    sql += ' ignore ';
  }

  sql += ` into ${table} (${columns.map((d) => knex.raw('??', d.name)).join(',')}) values ${rows.join(',')}`;

  if (bool(upsert, false)) {
    sql += ` ${worker.onDuplicate()} ${columns.map((column) => {
      const n = column.name;

      return `${knex.raw('??', n)}=${worker.onDuplicateFieldValue(knex.raw('??', n))}`;
    }).filter(Boolean).join(',')}`;

    // Not everything supports returning fields, but if it does ...
    if (returning.length > 0) sql += ` returning ${returning.map((d) => this.escapeColumn(d))}`;
  }

  return sql;
};

Worker.prototype.insertFromStream = async function (options) {
  const worker = this;
  const desc = await this.describe(options);
  const knex = await this.connect();
  debugMore('Connected, starting insertFromStream');
  return new Promise((resolve, reject) => {
    const table = this.escapeTable(options.table);
    let { stream } = options;
    if (!stream) {
      reject(new Error('stream is required for insertFromStream'));
      return;
    }
    if (Array.isArray(stream)) {
      debugMore(`Stream is an array, reading as an array of length ${stream.length}`);
      stream = Readable.from(stream);// Create a Readable stream from the array
    }

    const nullAsString = bool(options.nullAsString, false);
    const upsert = bool(options.upsert, false);

    let defaults = options.defaults || {};
    if (typeof defaults === 'string') defaults = JSON5.parse(defaults);

    const batchSize = parseInt(options.batchSize || 300, 10);
    let counter = 0;

    let columns = null;
    let rows = [];
    this.sqlCounter = this.sqlCounter || 0;
    function getIncludedObjectColumns(o) {
      return desc.columns.filter((f) => {
        // set the database appropriate name as well
        const sqlName = worker.getSQLName(f.name);
        if (o[f.name] !== undefined) {
          return true;
        } if (o[sqlName] !== undefined) {
          return true;
        }
        return false;
      });
    }
    const toSQL = through2.obj({}, function (o, enc, cb) {
      if (!o) return cb();

      this.sqlCounter += 1;

      if (Array.isArray(o)) {
        throw new Error('You should not pass an array into insertFromStream');
      }
      o.account_id = o.account_id || worker.accountId;

      // Support default values
      Object.keys(defaults).forEach((i) => {
        if (!o[i]) o[i] = defaults[i];
      });

      Object.keys(o).forEach((k) => {
        o[worker.getSQLName(k)] = o[k];
        o[k.trim()] = o[k];// Sometimes leading blanks are an issue
      });
      if (this.sqlCounter === 1) {
        debug('Beginning insert from stream to table', table, 'with columns ', desc.columns.map((d) => d.name)?.join(','), { upsert });
      }
      if (columns == null) {
        const includedObjectColumns = getIncludedObjectColumns(o);
        debugMore(`Running insertFromStream with columns ${includedObjectColumns.map((d) => `${d.name}(nullable:${d.nullable})`).join(',')}`, 'sample object:', o);
        if (includedObjectColumns.length === 0) {
          const msg = `insertFromStream to table ${table}: No columns found in object with keys: ${Object.keys(o).map((d) => `${d} unicode:${JSON.stringify(toCharCodes(d))}`)} that matches table description with columns:${desc.columns.map((d) => `${d.name} unicode:${JSON.stringify(toCharCodes(d.name))}`).join()}`;
          debug(JSON.stringify(desc));
          debug('Lookup table:', o);
          debug(msg);
          return cb(msg);
        }

        columns = includedObjectColumns;
      }

      if (rows.length >= batchSize) {
        /* Check to make sure we've got data in the right order here */
        const includedObjectColumns = getIncludedObjectColumns(o);
        const columnNames = columns.map((d) => d.name).join();
        const databaseFieldNames = desc.columns.map((d) => d.name).join();
        const objectFieldNames = includedObjectColumns.map((d) => d.name).join();
        if (columnNames !== objectFieldNames) {
          debug(
            `Creating an insert with ${rows.length} rows, compare:`,
            {
              table: options.table,
              databaseFieldNames,
              columns: columnNames,
              objectFieldNames,
              columnsAreEqual: columnNames === objectFieldNames,
              rowLength: (rows.length >= batchSize),
              object: o,
              undefinedColumns: Object.keys(o).filter((k) => o[k] === undefined).join(','),
            },
          );
          throw new Error("Cowardly failing, this is a developer issue. Inserting column names don't match exactly the available object column names, you probably have an undefined value inbound, check logs.");
        }
        if (rows.length > 0) {
          try {
            const sql = worker.buildInsertSql({
              knex, table, columns, rows, upsert,
            });
            if ((counter % 50000 === 0) || (counter < 1000 && counter % 200 === 0)) debug(`Inserting ${rows.length} rows, Total:${counter}`);

            this.push(sql);
          } catch (e) {
            return cb(e);
          }
        }
        rows = [];
        columns = includedObjectColumns;
      }

      const values = columns.map((def) => {
        let val = o[def.name];
        if (val === undefined) val = o[worker.getSQLName(def.name)];// check the SQLized name

        let v = null;
        try {
          v = worker.stringToType(val, def.column_type, def.length, def.nullable, nullAsString);
        } catch (e) {
          throw new Error(`Error with column ${def.name}: ${e}`);
        }

        return knex.raw('?', [v]).toString().replace(/\?/g, '\\u003F');
      });
      rows.push(`(${values.join(',')})`);
      counter += 1;

      return cb();
    }, function (cb) {
      if (rows.length > 0) {
        try {
          const complete = worker.buildInsertSql({
            knex, table, columns, rows, upsert,
          });
          this.push(complete);
        } catch (e) {
          return cb(e);
        }
      }
      return cb();
    });

    let hasError = null;
    const insertSQL = through2.obj((o, enc, cb) => {
      worker.query(o).then(() => {
        cb();
      }).catch(cb);
    });

    stream
      .pipe(toSQL)
      .pipe(insertSQL)
      .on('finish', () => {
        if (hasError) {
          reject(hasError);
          return;
        }
        resolve({
          table: options.table,
          records: counter,
        });
      })
      .on('error', (e) => {
        debug('Error event fired');
        hasError = e;
        debug(e);
        return reject(e);
      });
  });
};
Worker.prototype.insertFromStream.metadata = {
  options: {
    table: {},
    stream: {},
  },
};

/* Standard tables have an id field that is used to */
Worker.prototype.upsertArray = async function ({ table, array }) {
  if (!Array.isArray(array)) throw new Error('an array is required to upsert');

  if (array.length === 0) return [];

  const desc = await this.describe({ table });
  const knex = await this.connect();
  if (!desc.columns?.length) {
    debug(desc);
    debug('Exising tables:', await this.tables());
    throw new Error(`Error describing ${table}, no columns`);
  }

  // Use the first object to define the columns we're trying to upsert
  // Otherwise we have to do much less efficient per-item updates.
  // If you need to only specify some values, a previous deduplication
  // run should pre-populate the correct values
  const ignore = ['created_at', 'modified_at'];// these are handled by the database, should not be upserted
  const includedColumns = desc.columns
    .filter((f) => f.name.indexOf('_hidden_') !== 0)
    .filter((f) => ignore.indexOf(f.name) < 0)
    .filter((f) => array[0][f.name] !== undefined);
  if (includedColumns.length === 0) {
    debug('Table columns:', desc.columns.map((d) => d.name).join(), 'data columns:', Object.keys(array[0]).join(','));
    throw new Error('The incoming data does not have any attributes that match column names in the database');
  }

  const rows = array.map((o) => {
    const values = includedColumns.map((def) => {
      const val = o[def.name];

      let v = null;
      try {
        v = this.stringToType(val, def.column_type, def.length, def.nullable);
        if (v === undefined) throw new Error('undefined returned');
      } catch (e) {
        info(e);
        info('First record used for included columns:', array[0]);
        throw new Error(`Error mapping string to value:  Column '${def.name}', type='${def.column_type}': ${e}, attempted val=${val}, object=${JSON.stringify(o)}`);
      }
      const s = knex.raw('?', [v]).toString();
      return s.replace(/\?/g, '\\u003F');
    });

    return `(${values.join(',')})`;
  });
  const sql = this.buildInsertSql({
    knex, table, columns: includedColumns, rows, upsert: true, returning: ['id'],
  });
  try {
    const { data } = await this.query(sql);
    data.forEach((d, i) => {
      // this could be int vs string
      // eslint-disable-next-line eqeqeq
      if (array[i].id && d.id != array[i].id) {
        debug({ d, i }, array[i]);
        throw new Error(`There was a problem upserting object with id ${array[i].id},invalid id returned`);
      }
      array[i].id = d.id;
    });
    return array;
  } catch (e) {
    info({
      table, includedColumns, rows, sql,
    });
    throw e;
  }
};

Worker.prototype.upsertTables = async function ({ tablesToUpsert }) {
  return Promise.all(Object.keys(tablesToUpsert)
    .map((table) => {
      const array = tablesToUpsert[table];
      return this.upsertArray({ table, array });
    }));
};

Worker.prototype.drop = async function ({ table }) {
  if (!table) throw new Error('table is required');
  const o = await this.query(`drop table if exists ${this.escapeTable(table)}`);
  return o;
};

Worker.prototype.drop.metadata = {
  bot: true,
  options: { table: { required: true } },
};

Worker.prototype.dropTables = async function (options) {
  const { tables, filter } = options;
  if (!tables && !filter) throw new Error('dropTables requires a tables list or a filter');
  const { tables: tableList } = await this.tables(options);
  await Promise.all(tableList.map(async (table) => this.drop({ table })));
  return { tables: tableList };
};
Worker.prototype.dropTables.metadata = {
  options: {
    tables: {},
    filter: {},
  },
};

Worker.prototype.truncate = async function ({ table }) {
  if (!table) throw new Error('table is required');

  return this.query(`truncate table ${this.escapeTable(table)}`);
};

Worker.prototype.truncate.metadata = {
  bot: true,
  options: { table: { required: true } },
};

const { withAnalysis } = require('../utilities/eql');

Worker.prototype.withAnalysis = function (options) {
  const worker = this;
  const {
    eql, table, baseTable, table_alias, defaultTable,
  } = options;
  return withAnalysis({
    eql,
    baseTable: table || baseTable,
    defaultTable: defaultTable || table_alias || table,
    columnFn: (f) => worker.escapeColumn(f),
    valueFn: (f) => worker.escapeValue(f),
    tableFn: (f) => worker.escapeTable(f),
    functions: worker.dialect.supportedFunctions(),
    date_expr: options.date_expr || ((node, internal) => {
      const { operator, left_side, interval } = node;
      // let fn = 'date_add';
      // if(operator == '-') fn = 'date_sub';
      // return `${fn}(${internal(left_side)}, interval ${internal(value)} ${unit})`;
      const { value, unit } = interval;
      const date = internal(left_side);
      return this.getDateArithmeticFunction(date, operator, internal(value), unit);
    }),
  });
};

Worker.prototype.transformEql = function (options) {
  const { eql, table, table_alias } = options;
  const result = this.withAnalysis({
    eql,
    baseTable: table,
    defaultTable: table_alias || table,
  });

  const { cleaned, refsByTable } = result;

  return { sql: cleaned, refsByTable };
};

Worker.prototype.transformEql.metadata = {
  options: {
    eql: { required: true },
    table: { required: true },
    table_alias: {},
  },
};

Worker.prototype.buildSqlFromEQLObject = async function (options) {
  const worker = this;
  const baseTable = options.table;
  const {
    subquery,
    conditions = [],
    groupBy: _groupBy = [],
    orderBy: _orderBy = [],
    fields,
    limit,
    offset,
  } = options;
  let { columns, joins = [] } = options;
  if (!columns && fields) columns = fields;// Some legacy formats still use 'fields'
  if (!columns || columns.length === 0) throw new Error("No columns or fields specified, at least specify columns=['*']");
  const dbWorker = this;

  async function toSql() {
    if (!baseTable) throw new Error('table required');
    let baseTableSql = null;
    const tableDefs = {};
    if (typeof subquery === 'object') {
      // this may be a subtable/subquery
      const alias = subquery.alias || baseTable;
      if (!alias) throw new Error('Subqueries require a table name');
      if (alias.indexOf('${') >= 0) throw new Error('When using subqueries, the non-subquery table act as an alias, and cannot have a merge column');
      // prefill so we don't check for the existence of this non-existing table
      tableDefs[alias] = { columns: [] };
      baseTableSql = await dbWorker.buildSqlFromEQLObject(subquery);
      baseTableSql = `(${baseTableSql}) as ${dbWorker.escapeColumn(alias)}`;
    } else {
      baseTableSql = dbWorker.escapeTable(baseTable);
    }

    async function getTableDef({ table }) {
      if (!tableDefs[table]) {
        tableDefs[table] = await dbWorker.describe({ table });
      }
      return tableDefs[table];
    }

    const tablesToCache = {};
    columns.forEach((f) => {
      tablesToCache[f.table || baseTable] = true;
    });

    await Promise.all(Object.keys(tablesToCache).map((table) => getTableDef({ table })));

    const aggregateFns = {
      NONE: async (x) => x,
      COUNT: async (x) => `count(${x})`,
      COUNT_DISTINCT: async (x) => `count(distinct ${x})`,
      SUM: async (x) => `sum(${x})`,
      AVERAGE: async (x) => `average(${x})`,
      MAX: async (x) => `MAX(${x})`,
      MIN: async (x) => `MIN(${x})`,
    };

    const sqlFunctions = worker.dialect.supportedFunctions();

    async function fromColumn(input, opts) {
      const {
        table = baseTable, column, aggregate = 'NONE', function: func = 'NONE', name, eql,
      } = input;
      // eslint-disable-next-line no-shadow
      const { ignore_alias = false, orderBy = false } = opts || {};
      if (!table) throw new Error(`Invalid column, no table:${JSON.stringify(input)}`);

      let result;

      debugMore('fromColumn', input, opts);
      if (eql) {
        debugMore('Checking eql', eql);
        if (!name && !ignore_alias) throw new Error(`Invalid column ${JSON.stringify(input)}, a name is required if using eql`);
        debugMore('Transforming eql', eql);
        // eslint-disable-next-line no-shadow
        result = await dbWorker.transformEql({ eql, table });
        // eslint-disable-next-line no-console
        if (ignore_alias) result = result.sql;
        else result = `${result.sql} as ${dbWorker.escapeValue(name)}`;
        debugMore('Finished eql');
      } else if (input === '*' || column === '*') {
        debugMore('Using a * column');
        result = `${dbWorker.escapeTable(table)}.*`;
      } else {
        const def = await getTableDef({ table });
        const columnDef = def.columns.find((x) => x.name === column);
        if (!columnDef) {
          // New behavior -- allow for extraneous columns
          // and ignore them if they're not in the table
          // But NOT if you use eql, that will error
          if (opts && opts.ignore_missing) return null;
          throw new Error(`no such column: ${column} for ${table} with input:${JSON.stringify(input)} and opts:${JSON.stringify(opts)}`);
        }

        const withFunction = await sqlFunctions[func](`${dbWorker.escapeTable(table)}.${dbWorker.escapeColumn(column)}`);
        result = await aggregateFns[aggregate](withFunction);
        if (name && !ignore_alias) result = `${result} as ${dbWorker.escapeColumn(name)}`;
      }

      if (orderBy && input.orderByDirection) {
        const o = input.orderByDirection.toLowerCase();
        if (o !== 'asc' && o !== 'desc') throw new Error(`Invalid - must be asc or desc, not: ${o}`);
        result = `${result} ${o}`;
      }

      return result;
    }

    async function fromConditionValue({ value, ref }) {
      let x;
      if (value) x = dbWorker.escapeValue(value.value);
      else if (ref) x = await fromColumn(ref);
      return x;
    }

    const conditionFns = {
      EQUALS: async ([v1, v2]) => `${v1} = ${v2}`,
      NOT_EQUALS: async ([v1, v2]) => `${v1} <> ${v2}`,

      LESS_THAN: async ([v1, v2]) => `${v1} < ${v2}`,
      LESS_THAN_OR_EQUAL: async ([v1, v2]) => `${v1} <= ${v2}`,

      GREATER_THAN: async ([v1, v2]) => `${v1} > ${v2}`,
      GREATER_THAN_OR_EQUAL: async ([v1, v2]) => `${v1} >= ${v2}`,

      CONTAINS: async ([v1, v2]) => `${v1} like concat('%',${v2},'%')`,
      DOES_NOT_CONTAIN: async ([v1, v2]) => `${v1} not like concat('%',${v2},'%')`,

      IS_NULL: async ([v1]) => `${v1} is null`,
      IS_NOT_NULL: async ([v1]) => `${v1} is not null`,
    };

    async function fromCondition({ values: raw = [], type, eql }) {
      if (eql) {
        const s = dbWorker.transformEql({ eql, table: baseTable }).sql;
        debugMore('Parsed eql:', { baseTable, eql }, s);
        return s;
      }
      if (!type) throw new Error(`Could not find a condition type for values:${JSON.stringify(raw)}`);

      const values = await Promise.all(raw.map(fromConditionValue));
      if (typeof conditionFns[type] !== 'function') {
        throw new Error(`Could not find function for type:${type}`);
      }
      return conditionFns[type](values);
    }
    debugMore('Checking columns');
    if (!columns || !columns.length) {
      columns = (await getTableDef({ table: baseTable }))
        .columns.map(({ name }) => ({ column: name }));
    }
    debugMore('Checking selections');
    const selections = (await Promise.all(
      columns.map((f) => fromColumn(f, { ignore_missing: true })),
    )).filter(Boolean);
    debugMore('Checking where clause');

    const whereClauseParts = await Promise.all((conditions || []).map(fromCondition));
    let whereClause = '';
    if (whereClauseParts.length) {
      whereClause = `where\n${whereClauseParts.join(' and\n')}`;
    }
    debugMore('Checking groupBy');
    const groupBy = await Promise.all(_groupBy.map((f) => fromColumn(f, { ignore_alias: true })));
    let groupByClause = '';
    if (groupBy.length) groupByClause = `group by ${groupBy.join(',').trim()}`;

    debugMore('Checking orderBy');
    const orderBy = await Promise.all(
      [].concat(_orderBy || []).map((f) => fromColumn(f, { orderBy: true, ignore_alias: true })),
    );

    let orderByClause = '';
    if (orderBy.length) orderByClause = `order by ${orderBy.join(',')}`;

    let joinClause = '';
    if (!joins) joins = [];
    if (joins.length) {
      joinClause = (await Promise.all(joins.map(async (j) => {
        if (typeof j === 'string') throw new Error(`Attempting to parse EQL -- Object required for table join that must include table and join_eql, invalid=${j}`);
        const { table, join_eql } = j;
        if (!table || !join_eql) throw new Error('Invalid join specification, must include table and join_eql');
        const alias = j.alias || table;

        const match = dbWorker.transformEql({ eql: join_eql, table: baseTable });

        return `left join ${dbWorker.escapeTable(table)} as ${dbWorker.escapeTable(alias)} on ${match.sql}`;
      }))).join('\n').trim();
    }
    debugMore('Constructing parts');

    let sql = [
      'select',
      selections.join(',\n'),
      `from ${baseTableSql}`,
      `${joinClause}`,
      `${whereClause}`,
      `${groupByClause}`,
      `${orderByClause}`,
    ].filter(Boolean).join('\n').trim();

    if (limit) sql = dbWorker.addLimit(sql, limit, offset);
    return sql.trim();
  }

  return toSql();
};
Worker.prototype.buildSqlFromEQLObject.metadata = {
  options: {},
};

Worker.prototype.analyze = async function describe(opts) {
  const { table } = opts;
  const { columns } = await this.describe({ table });
  const indexes = await this.indexes({ table });
  let orderBy = '';
  const pkey = indexes.find((d) => d.primary);
  // we add an order by to get the
  // MAX of the primary keys, so we don't unintentionally just grab small numbers
  if (pkey) {
    orderBy = ` ORDER BY ${pkey.columns.map((c) => `${this.escapeColumn(c)} DESC`)}`;
  }

  const stream = await this.stream({
    sql: // try to get a good spread of values
    `(select * from ${this.escapeTable(table)} limit 25000)
      union 
      (select * from ${this.escapeTable(table)} ${orderBy} limit 25000)`,
  });
  return analyzeStream({ stream, fieldHints: columns });
};

Worker.prototype.analyze.metadata = {
  options: {
    sql: {},
  },
};

Worker.prototype.getNativeCreateTable = async function (options) {
  const { data } = await this.query(`show create table ${this.escapeTable(options.table)}`);
  return { sql: data[0]['Create Table'] };
};
Worker.prototype.getNativeCreateTable.metadata = {
  options: {
    table: { required: true },
  },
};

Worker.prototype.showProcessList = async function (options) {
  let { data: d } = await this.query('show full processlist');

  d = d.filter((x) => x.Command !== 'Sleep').sort((a, b) => (parseInt(a.Time, 10) < parseInt(b.Time, 10) ? -1 : 1));

  d.forEach((r) => {
    r.Info = (r.Info || '').replace(/\n/g, ' ').replace(/\t/g, ' ').replace(/\\'/g, "'").replace(/[\s]{2,100}/g, ' ');
  });
  if (options.filter) {
    d = d.filter((s) => s.Info.indexOf(options.filter) >= 0);
  }
  return d;
};

Worker.prototype.showProcessList.metadata = {
  options: { filter: {} },
};

module.exports = Worker;
