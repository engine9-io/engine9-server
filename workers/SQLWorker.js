/* eslint-disable camelcase */
const util = require('util');
const info = require('debug')('info:SQLWorker');
const debug = require('debug')('SQLWorker');
const debugMore = require('debug')('debug:SQLWorker');

const Knex = require('knex');
const { Readable } = require('stream');
const through2 = require('through2');
const JSON5 = require('json5');// Useful for parsing extended JSON
const { bool, toCharCodes, parseRegExp } = require('../utilities');
const SQLTypes = require('./SQLTypes');

const BaseWorker = require('./BaseWorker');

require('dotenv').config({ path: '.env' });

function Worker(worker) {
  BaseWorker.call(this, worker);
  this.accountId = worker.accountId;
  if (!this.accountId) throw new Error('No accountId provided to SQLWorker constructor');
  if (worker.knex) {
    this.knex = worker.knex;
  } else {
    this.auth = {
      ...worker.auth,
    };
  }
}

util.inherits(Worker, BaseWorker);
Worker.metadata = {
  alias: 'sql',
};

Worker.defaultStandardColumn = {
  name: '',
  type: '',
  length: null,
  nullable: true,
  default_value: null,
  auto_increment: false,
};

Worker.prototype.connect = async function connect() {
  if (this.knex) return this.knex;
  const { accountId } = this;
  if (!accountId) throw new Error('accountId is required for connect method');

  let config = null;
  const s = this.auth.database_connection;
  if (!s) throw new Error(`SQLWorker Could not find database_connection settings in auth configuration with keys ${Object.keys(this.auth)}`);

  config = {
    client: 'mysql2',
    connection: s,
  };

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
Worker.prototype.query = async function (_sql, values = []) {
  let sql = _sql;
  if (typeof _sql !== 'string') sql = _sql.sql;
  if (!sql) throw new Error('No sql provided');
  const knex = await this.connect();
  const [data, columns] = await knex.raw(sql, values);
  return { data, columns };
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

  let d = await this.query(sql, values);
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

Worker.prototype.escapeValue = function (t) {
  return SQLTypes.mysql.escapeValue(t);
};
Worker.prototype.addLimit = function (sql, limit, offset) {
  return SQLTypes.mysql.addLimit(sql, limit, offset);
};

const tableNameMatch = /^[a-zA-Z0-9_]+$/;
Worker.prototype.escapeTable = function escapeTable(t) {
  if (!t.match(tableNameMatch)) throw new Error(`Invalid table name: ${t}`);
  return t;
};
// Engine 9 follows a very restrictive column name standard, intended for cross-compatability
// Thus, escaping column names is mostly about validating they're following the rules
const columnNameMatch = /^[a-zA-Z0-9_]+$/;
Worker.prototype.escapeColumn = function escapeColumn(t) {
  if (!t.match(columnNameMatch)) throw new Error(`Invalid field name: ${t}`);
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
  const { table } = opts;
  if (!table) throw new Error(`No table provided to describe with opts ${Object.keys(opts)}`);
  const sql = `select database() as DB,COLUMN_NAME,COLUMN_TYPE,DATA_TYPE,IS_NULLABLE,COLUMN_DEFAULT,CHARACTER_MAXIMUM_LENGTH,EXTRA FROM information_schema.columns WHERE  table_schema = Database() AND table_name = '${this.escapeTable(table)}' order by ORDINAL_POSITION`;

  const cols = (await this.query(sql)).data;
  if (cols.length === 0) throw new Error(`Could not find table ${table}`, { cause: 'DOES_NOT_EXIST' });
  // databases return back arbitrary capitalization from information_schema
  cols.forEach((c) => { Object.keys(c).forEach((k) => { c[k.toUpperCase()] = c[k]; }); });

  const results = {};
  results.database = cols[0].DB;
  results.columns = cols.map((d) => {
    let defaultValue = d.COLUMN_DEFAULT;
    const extra = d.EXTRA;
    const onUpdate = 'on update current_timestamp()';
    if (extra.toLowerCase().indexOf(onUpdate) >= 0) defaultValue = (`${defaultValue || ''} ${onUpdate}`).trim();
    if (defaultValue === 'NULL') defaultValue = null;
    if (defaultValue !== null) {
      const type = d.COLUMN_TYPE.toUpperCase();
      if (type === 'TINYINT(1)') {
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
      // extra: d.EXTRA, //not standardized
      default_value: defaultValue,
      auto_increment: (d.EXTRA || '').toUpperCase().indexOf('AUTO_INCREMENT') >= 0,
    };
    return SQLTypes.mysql.dialectToStandard(o, {} || Worker.defaultColumn);
  });
  return results;
};

Worker.prototype.describe.metadata = {
  options: {
    table: { required: true },
  },
};

Worker.prototype.getSupportedSQLFunctions = function () {
  return SQLTypes.mysql.supportedFunctions();
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
      return dt.toISOString().slice(0, -1);

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
    default:
  }
  return v;
};

Worker.prototype.getSQLName = function (n) {
  return n.trim().replace(/[^0-9a-zA-Z_-]/g, '_').toLowerCase();
};

Worker.prototype.createTable = async function ({
  table: name, columns, timestamps = false, indexes = [],
}) {
  if (!columns || columns.length === 0) throw new Error('columns are required to createTable');
  const knex = await this.connect();
  await knex.schema.createTable(name, (table) => {
    const noTypes = columns.filter((c) => !c.type);
    if (noTypes.length > 0) throw new Error(`No type for columns: ${columns.map((d) => d.name).join()}`);

    columns.forEach((c) => {
      const {
        method, args, nullable, unsigned, defaultValue, defaultRaw,
      } = SQLTypes.mysql.standardToKnex(c);
      debug(`Adding knex for column ${c.name}`, c, {
        method, args, nullable, unsigned, defaultValue, defaultRaw,
      });
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
        m.defaultTo(defaultValue);
      }
    });
    const primaries = columns.filter((d) => d.primary_key).map((c) => c.name);
    if (primaries.length > 0) table.primary(primaries);
    indexes.forEach((x) => {
      if (x.unique) {
        table.unique(x.columns);
      } else {
        table.index(x.columns);
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
  await knex.schema.alterTable(name, (table) => {
    const noTypes = columns.filter((c) => !c.type);
    if (noTypes.length > 0) throw new Error(`No type for columns: ${columns.map((d) => d.name).join()}`);

    columns.forEach((c) => {
      const {
        method, args, nullable, unsigned, defaultValue, defaultRaw,
      } = SQLTypes.mysql.standardToKnex(c);
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
      if (x.unique) {
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

  if (columns.length === 0) throw new Error('no columns set before createInsert was called');
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
  return new Promise((resolve, reject) => {
    const table = this.escapeTable(options.table);
    let { stream } = options;
    if (!stream) {
      reject(new Error('stream is required for insertFromStream'));
      return;
    }
    if (Array.isArray(stream)) {
      debug(`Stream is an array, reading as an array of length ${stream.length}`);
      stream = Readable.from(stream);// Create a Readable stream from the array
    }

    const nullAsString = bool(options.nullAsString, false);

    let defaults = options.defaults || {};
    if (typeof defaults === 'string') defaults = JSON5.parse(defaults);

    const batchSize = parseInt(options.batchSize || 3, 10);
    const counter = 0;

    let columns = null;
    let rows = [];
    let sqlCounter = 0;
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

      sqlCounter += 1;

      if (Array.isArray(o)) {
        throw new Error('You should not pass an array into insertFromStream');
      }
      o.account_id = o.account_id || worker.account_id || 'n/a';

      // Support default values
      Object.keys(defaults).forEach((i) => {
        if (!o[i]) o[i] = defaults[i];
      });

      Object.keys(o).forEach((k) => {
        o[worker.getSQLName(k)] = o[k];
        o[k.trim()] = o[k];// Sometimes leading blanks are an issue
      });
      if (sqlCounter === 1) {
        debug('Insert from stream to table ', table, desc.columns.map((d) => d.name)?.join(','));
      }
      if (columns == null) {
        const includedObjectColumns = getIncludedObjectColumns(o);
        debug(`Running insertFromStream with columns ${includedObjectColumns.map((d) => `${d.name}(nullable:${d.nullable})`).join(',')}`, 'sample object:', o);
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
            const sql = worker.createInsertSql({
              knex, table, columns, rows,
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

        return knex.raw('?', [v]);
      });

      rows.push(`(${values.join(',')})`);

      return cb();
    }, function (cb) {
      if (rows.length > 0) {
        try {
          const complete = worker.createInsertSql({
            knex, table, columns, rows,
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
        resolve(null, {
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

  // Use the first object to define the columns we're trying to upsert
  // Otherwise we have to do much less efficient per-item updates.
  // If you need to only specify some values, a previous deduplication
  // run should pre-populate the correct values
  const ignore = ['created_at', 'modified_at'];// these are handled by the database, should not be upserted
  const includedColumns = desc.columns
    .filter((f) => ignore.indexOf(f.name) < 0)
    .filter((f) => array[0][f.name] !== undefined);

  const rows = array.map((o) => {
    const values = includedColumns.map((def) => {
      const val = o[def.name];

      let v = null;
      try {
        v = this.stringToType(val, def.column_type, def.length, def.nullable);
      } catch (e) {
        info(e);
        throw new Error(`Error mapping string to value:  Column '${def.name}', type='${def.column_type}': ${e}, attempted val=${val}, object=${JSON.stringify(o)}`);
      }

      return knex.raw('?', [v]);
    });

    return `(${values.join(',')})`;
  });
  const sql = this.buildInsertSql({
    knex, table, columns: includedColumns, rows, upsert: true, returning: ['id'],
  });
  try {
    const { data } = await this.query(sql);
    data.forEach((d, i) => {
      if (array[i].id && d.id !== array[i].id) throw new Error(`There was a problem upserting object with id ${array[i].id},invalid id returned`);
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
    .map((table) => this.upsertArray({ table, array: tablesToUpsert[table] })));
};

Worker.prototype.drop = async function ({ table }) {
  if (!table) throw new Error('table is required');

  return this.query(`drop table if exists ${this.escapeTable(table)}`);
};

Worker.prototype.drop.metadata = {
  bot: true,
  options: { table: { required: true } },
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
    functions: worker.getSupportedSQLFunctions(),
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
    group_by = [],
    order_by = [],
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

    const functionFns = worker.getSupportedSQLFunctions();

    async function fromColumn(input, opts) {
      const {
        table = baseTable, column, aggregate = 'NONE', function: func = 'NONE', alias, eql,
      } = input;
      // eslint-disable-next-line no-shadow
      const { ignore_alias = false, order_by = false } = opts || {};
      if (!table) throw new Error(`Invalid column, no table:${JSON.stringify(input)}`);

      let result;

      debugMore('fromColumn', input, opts);
      if (eql) {
        debugMore('Checking eql', eql);
        if (!alias && !ignore_alias) throw new Error('alias is required if using eql');
        debugMore('Transforming eql', eql);
        // eslint-disable-next-line no-shadow
        result = await dbWorker.transformEql({ eql, table });
        // eslint-disable-next-line no-console
        if (ignore_alias) result = result.sql;
        else result = `${result.sql} as ${dbWorker.escapeColumn(alias)}`;
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

        const withFunction = await functionFns[func](`${dbWorker.escapeTable(table)}.${dbWorker.escapeColumn(column)}`);
        result = await aggregateFns[aggregate](withFunction);
        if (alias && !ignore_alias) result = `${result} as ${dbWorker.escapeColumn(alias)}`;
      }

      if (order_by && input.order_by_direction) {
        const o = input.order_by_direction.toLowerCase();
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
    const groupBy = await Promise.all(group_by.map((f) => fromColumn(f, { ignore_alias: true })));
    let groupByClause = '';
    if (groupBy.length) groupByClause = `group by ${groupBy.join(',').trim()}`;

    debugMore('Checking orderBy');
    const orderBy = await Promise.all(
      (order_by || []).map((f) => fromColumn(f, { order_by: true, ignore_alias: true })),
    );

    let orderByClause = '';
    if (orderBy.length) orderByClause = `order by ${orderBy.join(',')}`;

    let joinClause = '';
    if (!joins) joins = [];
    if (joins.length) {
      joinClause = (await Promise.all(joins.map(async (j) => {
        const { alias, target: _target, match_eql } = j;
        const target = _target || alias;
        // console.log("match_eql=",match_eql);
        const match = dbWorker.transformEql({ eql: match_eql, table: baseTable });

        return `left join ${dbWorker.escapeTable(target)} as ${dbWorker.escapeTable(alias)} on ${match.sql}`;
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
  bot: true,
  options: {},
};

module.exports = Worker;
