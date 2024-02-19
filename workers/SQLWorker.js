const util = require('util');
const debug = require('debug')('SQLWorker');
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
Worker.prototype.query = async function (_sql) {
  let sql = _sql;
  if (typeof _sql !== 'string') sql = _sql.sql;
  const knex = await this.connect();
  const [data, columns] = await knex.raw(sql);
  return { data, columns };
};
Worker.prototype.query.metadata = {
  options: {
    sql: {},
  },
};

Worker.prototype.tables = async function f(options) {
  let sql = 'select TABLE_NAME from information_schema.tables where table_schema=';
  if (options.database) {
    sql += this.escapeValue(options.database);
  } else {
    sql += 'database()';
  }
  if (options.type === 'view') {
    sql += " and table_type='VIEW'";
  } else if (options.type === 'table') {
    sql += " and table_type='BASE TABLE'";
  }

  let d = await this.query(sql);
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

  return {
    table,
    indexes: d[0].map((i) => ({
      index_name: i.INDEX_NAME || i.index_name,
      columns: i.columns,
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

Worker.prototype.describe = async function describe(opts) {
  const { table } = opts;
  if (!table) throw new Error(`No table provided to describe with opts ${Object.keys(opts)}`);
  const sql = `select database() as db,column_name,column_type,data_type,is_nullable,column_default,extra,character_maximum_length,extra FROM information_schema.columns WHERE  table_schema = Database() AND table_name = '${this.escapeTable(table)}' order by ORDINAL_POSITION`;

  const cols = (await this.query(sql)).data;
  if (cols.length === 0) throw new Error(`Could not find table ${table}`, { cause: 'DOES_NOT_EXIST' });
  // databases return back arbitrary capitalization from information_schema
  cols.forEach((c) => { Object.keys(c).forEach((k) => { c[k.toUpperCase()] = c[k]; }); });

  const results = {};
  results.database = cols[0].db;
  results.columns = cols.map((d) => {
    let defaultValue = d.COLUMN_DEFAULT;
    const extra = d.EXTRA.toUpperCase();
    const onUpdate = 'ON UPDATE CURRENT_TIMESTAMP';
    if (extra.indexOf(onUpdate) >= 0) defaultValue = (`${defaultValue || ''} ${onUpdate}`).trim();
    if (defaultValue !== null) {
      const type = d.COLUMN_TYPE.toUpperCase();
      if (type.indexOf('INT') === 0
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

Worker.prototype.createTable = async function ({ table: name, columns, timestamps = false }) {
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
      console.log('Setting default value to ', defaultValue);
      if (defaultRaw !== undefined) {
        const allowedRaw = ['CURRENT_TIMESTAMP',
          'CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'];
        if (allowedRaw.indexOf(defaultRaw) < 0) throw new Error(`Invalid knex raw value:${defaultRaw}`);
        m.defaultTo(knex.raw(defaultRaw));
      } else if (defaultValue !== undefined) {
        m.defaultTo(defaultValue);
      }
    });
    const primaries = columns.filter((d) => d.primary_key).map((c) => c.name);
    if (primaries.length > 0) table.primary(primaries);
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

Worker.prototype.onDuplicate = function () { return 'on duplicate key update'; };
Worker.prototype.onDuplicateFieldValue = function (f) { return `VALUES(${f})`; };

Worker.prototype.createInsertSql = function (options) {
  const worker = this;
  const {
    knex, table, columns, rows,
  } = options;

  if (columns.length === 0) throw new Error('no columns set before createInsert was called');
  let sql = 'INSERT';
  if (bool(options.ignore_dupes, false)) {
    sql += ' ignore ';
  }

  sql += ` into ${table} (${columns.map((d) => knex.raw('??', d.name)).join(',')}) values ${rows.join(',')}`;

  if (bool(options.upsert, false)) {
    sql += ` ${worker.onDuplicate()} ${columns.map((column) => {
      const n = column.name;

      return `${knex.raw('??', n)}=${worker.onDuplicateFieldValue(knex.raw('??', n))}`;
    }).filter(Boolean).join(',')}`;
  }

  return sql;
};

Worker.prototype.insertFromStream = async function (options) {
  const desc = await this.describe(options);
  const knex = await this.connect();
  return new Promise((resolve, reject) => {
    const worker = this;
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
          v = worker.stringToType(val, def.data_type, def.length, def.nullable, nullAsString);
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

module.exports = Worker;
