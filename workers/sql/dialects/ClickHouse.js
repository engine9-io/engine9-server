// for escaping logic, default to mysql
const mysql = require('mysql2');

const types = [
  {
    type: 'id',
    column_type: 'Int64',
    nullable: false,
    knex_method: 'specificType',
    knex_args: (() => (['Int64'])),
  },
  {
    // person id is a very specific foreign id
    // person_id type definition is ALWAYS the same, regardless if it's a primary key or not
    // that way we never have to deal with null cases, and different platforms can rely on it having
    // a non-null value
    // Even though there's situations where it's not set, that value is then 0
    type: 'person_id',
    column_type: 'Int64',
    nullable: false,
    default_value: 0,
    knex_method: 'specificType',
    knex_args: (() => (['Int64'])),
  },
  {
    // foreign ids can be nullable
    type: 'foreign_id',
    column_type: 'Int64',
    nullable: true,
    knex_method: 'specificType',
    knex_args: (() => (['Int64'])),
  },
  { // A string identifier, similar to a string,
    // but can't be null, defaults to '', 64 chars to join with hashes
    type: 'id_string',
    column_type: 'String',
    length: 64,
    nullable: false,
    default_value: '',
    knex_method: 'specificType',
    knex_args: ['String'],
  },
  { // A uuid identifier, similar to a string, but can't be null

    type: 'id_uuid',
    column_type: 'UUID',
    default_value: '00000000-0000-0000-0000-000000000000',
    knex_method: 'specificType',
    knex_args: ['UUID'],
    nullable: false,
  },
  {
    type: 'string',
    column_type: 'String',
    length: 255,
    knex_method: 'specificType',
    knex_args: ['String'],
  },
  {
    type: 'hash',
    column_type: 'String',
    length: 64,
    nullable: false,
    default_value: '',
    knex_method: 'specificType',
    knex_args: ['String'],
  },

  {
    type: 'tinyint',
    column_type: 'Int8',
    knex_method: 'specificType',
    knex_args: (() => (['Int8'])),
    nullable: false,
  },
  {
    type: 'smallint',
    column_type: 'Int16',
    knex_method: 'specificType',
    knex_args: (() => (['Int16'])),
    nullable: false,

  },
  {
    type: 'int',
    column_type: 'Int32',
    knex_method: 'specificType',
    knex_args: ['Int32'],
    nullable: false,

  },
  {
    type: 'bigint',
    column_type: 'Int64',
    knex_method: 'specificType',
    knex_args: (() => (['Int64'])),
    nullable: false,
  },

  /* decimal(19,2) is chosen for currency
  as 4 digit currencies are rare enough to warrant another type */
  {
    type: 'currency', column_type: 'Decimal(19,2)', knex_method: 'specificType', knex_args: () => (['Decimal(19, 2)']),
  },
  {
    type: 'decimal', column_type: 'Decimal', knex_method: 'specificType', knex_args: (() => (['Decimal(19, 4)'])),
  },
  {
    type: 'double',
    column_type: 'Float64',
    knex_method: 'specificType',
    knex_args: ['Float64'],

  },
  {
    type: 'boolean',
    column_type: 'Boolean',
    // nullable: true, //don't set this, otherwise we can't look up type boolean
    //  Defaults to nullable anyhow
    knex_method: 'specificType',
    knex_args: ['Boolean'],
  },
  {
    type: 'text',
    column_type: 'String',
    knex_method: 'specificType',
    knex_args: ['String'],
  },
  {
    type: 'json',
    column_type: 'JSON',
    knex_method: 'specificType',
    knex_args: ['JSON'],
  },
  {
    type: 'created_at',
    column_type: 'DateTime',

    nullable: false,
    knex_method: 'specificType',
    knex_args: ['DateTime'],
    default_value: '1970-01-01 00:00:00',
    // knex_default_raw: 'current_timestamp()',
  },
  {
    type: 'modified_at',
    column_type: 'DateTime',
    default_value: '1970-01-01 00:00:00',
    nullable: false,
    knex_method: 'specificType',
    knex_args: ['DateTime'],
    knex_default_raw: 'current_timestamp() on update current_timestamp()',
  },
  {
    type: 'url',
    column_type: 'String',
    knex_method: 'specificType',
    knex_args: ['String'],
  },
  {
    type: 'date',
    column_type: 'Date',
    knex_method: 'specificType',
    knex_args: ['Date'],

  },
  {
    type: 'datetime',
    column_type: 'DateTime',
    knex_method: 'specificType',
    knex_args: ['DateTime'],

  },
  {
    type: 'timestamp',
    column_type: 'DateTime',
    knex_method: 'specificType',
    knex_args: ['DateTime'],
  },
  {
    type: 'time',
    column_type: 'Time',
    knex_method: 'specificType',
    knex_args: ['Time'],
  },
  {
    type: 'enum',
    column_type: 'LowCardinality(String)',
    knex_method: 'specificType',
    knex_args: ['LowCardinality(String)'],
  },
  // foreign ids can be nullable
  {
    type: 'foreign_uuid',
    column_type: 'UUID',
    default_value: '00000000-0000-0000-0000-000000000000',
    knex_method: 'specificType',
    knex_args: ['UUID'],
    nullable: true,
  },
  {
    type: 'uuid',
    column_type: 'UUID',
    default_value: '00000000-0000-0000-0000-000000000000',
    knex_method: 'specificType',
    knex_args: (() => (['UUID'])),
  },
];
function isInt(s) { return Number.isInteger(typeof s === 'number' ? s : parseFloat(s)); }

module.exports = {
  name: 'ClickHouse',
  getType(type) {
    return types.find((t) => t.type === type);
  },
  standardToKnex(col) { // return {method,args} for knex
    // The name of the knex methods is ... inconsistent
    const { type } = col;
    const typeDef = types.find((t) => t.type === type);
    if (!typeDef) throw new Error(`Could not find mysql type ${type}`);
    let { nullable } = col;
    if (nullable === undefined) {
      nullable = typeDef.nullable;
    }
    if (nullable === undefined) {
      nullable = true;
    }

    return {
      method: typeDef.knex_method,
      args: typeof typeDef.knex_args === 'function' ? typeDef.knex_args(col) : (typeDef.knex_args || []),
      unsigned: typeDef.unsigned || false,
      nullable,
      defaultValue: typeDef.default_value !== undefined
        ? typeDef.default_value : typeDef.knex_default,
      // raw values should always be defined strings in this file, not a function
      defaultRaw: typeDef.knex_default_raw,
    };
  },
  dialectToStandard(o, defaultColumn) {
    const input = { ...defaultColumn, ...o };
    // test first for strings
    if (input.column_type.indexOf('String') === 0) {
      // this is a string, which can have all the variations
      input.column_type = 'string';
      if (input.default_value
          && typeof input.default_value === 'string'
          && input.default_value.match(/^'.*'$/)) {
        input.default_value = input.default_value.slice(1, -1);
      }
      return { ...types.find((t) => t.type === 'string'), ...input };
    }

    if (input.column_type.toLowerCase().indexOf('enum') === 0) {
      // example enum('','remote_person_id','email_hash_v1','phone_hash_v1')
      input.values = input.column_type.slice(5, -1).split(',').map((d) => d.slice(1, -1));
      input.column_type = 'enum';
      if (input.default_value
          && typeof input.default_value === 'string'
          && input.default_value.match(/^'.*'$/)) {
        input.default_value = input.default_value.slice(1, -1);
      }
    }

    if (input.column_type.slice(-9).toLowerCase() === ' unsigned') {
      input.column_type = input.column_type.slice(0, -9);
      input.unsigned = true;
    }
    if (input.column_type.indexOf('Int8') === 0) {
      input.column_type = 'Int8';
    }
    if (input.column_type.indexOf('Int64') === 0) {
      input.column_type = 'Int64';
    }
    if (input.column_type === 'LowCardinality(String)') {
      input.column_type = 'String';
    }
    if (input.column_type.indexOf('Int32') === 0
      || input.column_type.indexOf('Int16') === 0 // smallint isn't small enough to matter significantly
      || input.column_type.indexOf('UInt') === 0
    ) {
      input.column_type = 'Int32';
    }
    if (input.column_type.indexOf('Decimal') === 0) {
      input.column_type = 'Decimal';
    }
    /* some engines have commented out descriptions */
    if (input.column_type.indexOf('/*') > 0) {
      [input.column_type] = input.column_type.split(' ');
    }
    const log = [];
    const typeDef = types.find(
      (type) => {
        const unmatchedAttributes = Object.keys(type).map((attr) => {
          if (attr === 'type' || attr.indexOf('knex_') === 0) return false;// ignore these
          if (type[attr] === input[attr]) {
            return false;
          }
          return `${attr}:${type[attr]} !== ${input[attr]}`;
        }).filter(Boolean);
        if (unmatchedAttributes.length > 0) {
          log.push({ type: type.type, unmatchedAttributes });
          return false;
        }
        return true;
      },
    );
    if (!typeDef) {
      throw new Error(`dialectToStandard: Could not find column type that matches ${JSON.stringify(input)} \n${log.map((s) => JSON.stringify(s)).join('\n')}`);
    }

    return Object.assign(input, typeDef);
  },
  escapeColumn(value) {
    return mysql.escapeId(value);
  },
  escapeValue(value) {
    return mysql.escape(value);
  },
  addLimit(_sql, limit, offset) {
    if (!limit) return _sql;
    let sql = _sql;
    // make sure it's an integer, and defined
    if (isInt(limit)) {
      sql += ` limit ${limit}`;
      if (offset) sql += ` offset ${offset}`;
    } else if (limit) {
      throw new Error(`Invalid limit:${limit}`);
    }
    return sql;
  },

  supportedFunctions() {
    const scope = this;
    const columnNameMatch = /^[a-zA-Z0-9_]+$/;
    function escapeColumn(t) {
      if (!t.match(columnNameMatch)) throw new Error(`Invalid column name: ${t}`);
      return t;
    }
    this.getDayFunction = function (_column, skipEscape) {
      let column = _column;
      if (!skipEscape) column = this.escapeColumn(column);
      return `DATE(${column})`;
    };

    this.getQuarterFunction = function (_column, skipEscape) {
      let column = _column;
      // Works on legacy SQLServer and modern
      if (!skipEscape) column = this.escapeColumn(column);
      return `CONCAT(YEAR(${column}),'-Q',QUARTER(${column}))`;
    };

    this.getStartOfQuarterFunction = function (_column, skipEscape) {
      let column = _column;
      if (!skipEscape) column = this.escapeColumn(column);
      return `MAKEDATE(YEAR(${column}), 1) + INTERVAL QUARTER(${column})-1 QUARTER`;
    };

    this.getDateIntervalFunction = function (column, days) {
      return `${this.escapeColumn(column)}+ interval ${days} day`;
    };

    this.getHourIntervalFunction = function (column, hours) {
      return `${this.escapeColumn(column)}+ interval ${hours} hour`;
    };

    // Short date function -- particularly for
    // Google that mishandles date/datetime types in mysql, and needs YYYYMMDD format
    this.getShortDateFunction = function (_column, skipEscape) {
      let column = _column;
      if (!skipEscape) column = this.escapeColumn(column);
      return `DATE_FORMAT(${column},'%Y%m%d')`;
    };

    this.getWeekFunction = function (_column, skipEscape) {
      let column = _column;
      if (!skipEscape) column = this.escapeColumn(column);
      return `FROM_DAYS(TO_DAYS(${column}) -MOD(TO_DAYS(${column}) -1, 7))`;
    };

    this.getMonthNameFunction = function (_column, skipEscape) {
      let column = _column;
      if (!skipEscape) column = this.escapeColumn(column);
      return `MONTHNAME(${column})`;
    };

    /*
      Gets the timezone, defaults to the timezone from this bot
      */
    this.getTimezoneFunction = function (_column, timezone, skipEscape) {
      let column = _column;
      if (!skipEscape) column = this.escapeColumn(column);
      const tz = timezone || (this.auth || {}).timezone || 'America/New_York';
      return `CONVERT_TZ(${column},'UTC',${this.escapeValue(tz)})`;
    };

    this.getMonthFunction = function (_column, skipEscape) {
      let column = _column;
      if (!skipEscape) column = this.escapeColumn(column);
      return `DATE_ADD(LAST_DAY(DATE_SUB(${column}, interval 31 day)), interval 1 day)`;
    };

    this.getStartOfYearFunction = function (_column, skipEscape) {
      let column = _column;
      if (!skipEscape) column = this.escapeColumn(column);
      return `CONCAT(YEAR(${column}),'-01-01')`;
    };

    this.getYearFunction = function (_column, skipEscape) {
      let column = _column;
      if (!skipEscape) column = this.escapeColumn(column);
      return `YEAR(${column})`;
    };

    /* Calculate the fiscal year from a date */
    this.getFiscalYearFunction = function (_column, fiscalYear, skipEscape) {
      let column = _column;
      if (!fiscalYear) return scope.getYearFunction(column, skipEscape);
      const { month, day } = scope.getFiscalYearUtilities(fiscalYear);
      if (month === 1 && day === 1) return scope.getYearFunction(column, skipEscape);
      if (!skipEscape) column = escapeColumn(column);
      return `year(${column} + interval ${13 - month} month - interval ${day - 1} day)`;
    };

    this.getDayOfYearFunction = function (_column, skipEscape) {
      let column = _column;
      if (!skipEscape) column = escapeColumn(column);
      return `DAYOFYEAR(${column})`;
    };

    this.getDayOfFiscalYearFunction = function (_column, fiscalYear, skipEscape) {
      let column = _column;
      if (!fiscalYear) return scope.getYearFunction(column, skipEscape);
      const { month, day } = scope.getFiscalYearUtilities(fiscalYear);
      if (month === 1 && day === 1) return scope.getDayOfYearFunction(column, skipEscape);
      if (!skipEscape) column = escapeColumn(column);
      return this.getDayDiffFunction(column, `CONCAT(${this.getFiscalYearFunction(column, fiscalYear, true)}-1,'-${fiscalYear}')`);
    };

    this.getNowFunction = function () {
      return 'NOW()';
    };
    this.getDayDiffFunction = function (a, b) {
      return `DATEDIFF(${a},${b})`;
    };

    this.getDateFunction = function (x) {
      return `DATE(${x})`;
    };

    this.getHashV1Function = function (x) {
      return `sha2(lower(trim(coalesce(${x},''))),256)`;
    };
    this.getEmailDomainFunction = function (x) {
      return `COALESCE(LOWER(SUBSTRING(${x}, LOCATE('@', ${x}) + 1)),'')`;
    };

    this.getDateSubFunction = function (date, value, type) {
      return `date_sub(${date}, interval ${value} ${type})`;
    };

    this.getDateArithmeticFunction = function (date, operator, value, intervalType) {
      let fn = 'date_add';
      if (operator === '-') fn = 'date_sub';
      return `${fn}(${date}, interval ${value} ${intervalType})`;
    };

    this.getStringIndexOfFunction = function (str, find, pos) {
      if (pos == null) return `LOCATE(${str},${find})`;
      return `LOCATE(${str},${find},${pos})`;
    };

    this.getFullTextBoolean = function (column, match) {
      return `MATCH(${column}) against (${match} IN BOOLEAN MODE)`;
    };

    this.getUUIDFunction = function () { return 'UUID()'; };

    this.getIfNullFunction = function (check, value) {
      return `IFNULL(${check}, ${value})`;
    };

    this.getCastIntFunction = function (value) {
      return `CAST(${value} AS SIGNED)`;
    };

    this.getTrimFunction = function (value) {
      return `TRIM(${value})`;
    };
    this.getConcatFunction = function (...rest) {
      let value = rest;
      if (value.length === 1 && Array.isArray(value[0])) [value] = value;
      return `CONCAT(${value.join(',')})`;
    };
    this.getShaLength40Function = function (_value) {
      let value = _value;
      if (Array.isArray(value)) {
        value = this.getConcatFunction(value);
      }
      return `UPPER(SHA(${value}))`;
    };
    this.getRegexpFunction = function (value, pattern) {
      return `${value} regexp ${pattern}`;
    };
    const functions = {
      DISTINCT: 1,

      COUNT: 1,
      SUM: 1,
      AVG: 1,
      MIN: 1,
      MAX: 1,

      NONE: (x) => x,
      DAY: (x) => this.getDayFunction(x, true),
      WEEK: (x) => this.getWeekFunction(x, true),
      MONTH: (x) => this.getMonthFunction(x, true),
      YEAR: (x) => this.getYearFunction(x, true),
      FISCAL_YEAR: (x) => this.getFiscalYearFunction(x, this.fiscalYear || '01-01', true),
      DAY_OF_YEAR: (x) => this.getDayOfYearFunction(x, true),
      DAY_OF_FISCAL_YEAR: (x) => this.getDayOfFiscalYearFunction(x, this.fiscalYear || '01-01', true),
      NOW: () => this.getNowFunction(),
      DATE: (x) => this.getDateFunction(x),
      LOCATE: ([x, y, z]) => this.getStringIndexOfFunction(x, y, z),
      FULLTEXT_BOOLEAN: ([x, y]) => this.getFullTextBoolean(x, y),
      REGEXP_LIKE: ([x, y]) => this.getRegexpFunction(x, y),
      NULLIF: 1,
      IFNULL: 1,
    };
    return functions;
  },
};
