/* eslint-disable camelcase */
const util = require('node:util');
const debug = require('debug')('PersonWorker');
const { withAnalysis } = require('./e9ql');

const SQLWorker = require('./SQLWorker');

function Worker(worker) {
  SQLWorker.call(this, worker);
}

util.inherits(Worker, SQLWorker);

function asPromise(wrapper) {
  return new Promise((resolve, reject) => {
    wrapper((e, r) => {
      if (e) return reject(e);
      return resolve(r);
    });
  });
}

Worker.prototype.withAnalysis = function (options) {
  const {
    e9ql, table, baseTable, table_alias, defaultTable,
  } = options;
  return withAnalysis({
    e9ql,
    baseTable: table || baseTable,
    defaultTable: defaultTable || table_alias || table,
    fieldFn: (f) => this.escapeField(f),
    valueFn: (f) => this.escapeValue(f),
    tableFn: (f) => this.escapeTable(f),
    functions: this.getSupportede9qlFunctions(),
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

Worker.prototype.transformE9ql = function (options) {
  const { e9ql, table, table_alias } = options;
  const result = this.withAnalysis({
    e9ql,
    baseTable: table,
    defaultTable: table_alias || table,
  });

  const { cleaned, refsByTable } = result;

  return { sql: cleaned, refsByTable };
};

Worker.prototype.transformE9ql.metadata = {
  options: {
    e9ql: { required: true },
    table: { required: true },
    table_alias: {},
  },
};

Worker.prototype.buildSqlFromQuery = async function (options) {
  const baseTable = options.table;
  const {
    subquery,
    conditions = [],
    group_by = [],
    order_by = [],

    limit,
    offset,
  } = options;
  let { fields, joins = [] } = options;
  const dbWorker = this;

  async function toSql() {
    if (!baseTable) throw new Error('table required');
    let baseTableSql = null;
    const tableDefs = {};
    if (typeof subquery === 'object') {
      // this may be a subtable/subquery
      const alias = baseTable;
      if (!alias) throw new Error('Subqueries require a table name');
      if (alias.indexOf('${') >= 0) throw new Error('When using subqueries, the non-subquery table act as an alias, and cannot have a merge field');
      // prefill so we don't check for the existence of this non-existing table
      tableDefs[alias] = { fields: [] };
      baseTableSql = await asPromise((cb) => dbWorker.buildSqlFromQuery(subquery, cb));
      baseTableSql = `(${baseTableSql}) as ${dbWorker.escapeField(alias)}`;
    } else {
      baseTableSql = dbWorker.escapeTable(baseTable);
    }

    async function getTableDef({ table }) {
      if (!tableDefs[table]) {
        tableDefs[table] = await asPromise((cb) => dbWorker.describe({ table }, (e, r) => {
          if (e) {
            debug('Error getting table ', table, ' from options:', options);
            return cb(e);
          }
          if (!r) return cb(`no table found: ${table}`);
          return cb(null, r);
        }));
      }
      return tableDefs[table];
    }

    const tablesToCache = {};
    fields.forEach((f) => {
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

    const functionFns = {
      NONE: async (x) => x,
      DAY: async (x) => dbWorker.getDayFunction(x, true),
      WEEK: async (x) => dbWorker.getWeekFunction(x, true),
      MONTH: async (x) => dbWorker.getMonthFunction(x, true),
      YEAR: async (x) => dbWorker.getYearFunction(x, true),
      NOW: async () => dbWorker.getNowFunction(),
      DAY_OF_YEAR: async (x) => dbWorker.getDayOfYearFunction(x, dbWorker.fiscal_year, true),
      DAY_OF_FISCAL_YEAR: async (x) => {
        dbWorker.getDayOfFiscalYearFunction(x, dbWorker.fiscal_year, true);
      },
    };

    async function fromField(input, opts) {
      const {
        table = baseTable, field, aggregate = 'NONE', function: func = 'NONE', alias, e9ql,
      } = input;
      // eslint-disable-next-line no-shadow
      const { ignore_alias = false, order_by = false } = opts || {};
      if (!table) throw new Error(`Invalid field, no table:${JSON.stringify(input)}`);

      let result;
      if (e9ql) {
        if (!alias && !ignore_alias) throw new Error('alias is required if using e9ql');
        // eslint-disable-next-line no-shadow
        result = await asPromise((cb) => dbWorker.transformE9ql({ e9ql, table }, cb));
        // eslint-disable-next-line no-console
        if (ignore_alias) result = result.sql;
        else result = `${result.sql} as ${dbWorker.escapeField(alias)}`;
      } else {
        const def = await getTableDef({ table });
        const fieldDef = def.fields.find((x) => x.name === field);
        if (!fieldDef) {
          // New behavior -- allow for extraneous fields and ignore them if they're not in the table
          // But NOT if you use e9ql, that will error
          if (opts && opts.ignore_missing) return null;
          throw new Error(`no such field: ${field} for ${table} with input:${JSON.stringify(input)} and opts:${JSON.stringify(opts)}`);
        }

        const withFunction = await functionFns[func](`${dbWorker.escapeTable(table)}.${dbWorker.escapeField(field)}`);
        result = await aggregateFns[aggregate](withFunction);
        if (alias && !ignore_alias) result = `${result} as ${dbWorker.escapeField(alias)}`;
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
      else if (ref) x = await fromField(ref);
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

    async function fromCondition({ values: raw = [], type, e9ql }) {
      if (e9ql) {
        return dbWorker.transformE9ql({ e9ql, table: baseTable }).sql;
      }

      const values = await Promise.all(raw.map(fromConditionValue));
      return conditionFns[type](values);
    }

    if (!fields || !fields.length) {
      fields = (await getTableDef({ table: baseTable }))
        .fields.map(({ name }) => ({ field: name }));
    }
    const selections = (await Promise.all(
      fields.map((f) => fromField(f, { ignore_missing: true })),
    )).filter(Boolean);

    const whereClauseParts = await Promise.all((conditions || []).map(fromCondition));
    let whereClause = '';
    if (whereClauseParts.length) {
      whereClause = `where\n${whereClauseParts.join(' and\n')}`;
    }

    const groupBy = await Promise.all(group_by.map((f) => fromField(f, { ignore_alias: true })));
    let groupByClause = '';
    if (groupBy.length) groupByClause = `group by ${groupBy.join(',').trim()}`;

    const orderBy = await Promise.all(
      (order_by || []).map((f) => fromField(f, { order_by: true, ignore_alias: true })),
    );

    let orderByClause = '';
    if (orderBy.length) orderByClause = `order by ${orderBy.join(',')}`;

    let joinClause = '';
    if (!joins) joins = [];
    if (joins.length) {
      joinClause = (await Promise.all(joins.map(async (j) => {
        const { alias, target: _target, match_e9ql } = j;
        const target = _target || alias;
        // console.log("match_e9ql=",match_e9ql);
        const match = dbWorker.transformE9ql({ e9ql: match_e9ql, table: baseTable });

        return `left join ${dbWorker.escapeTable(target)} as ${dbWorker.escapeTable(alias)} on ${match.sql}`;
      }))).join('\n').trim();
    }
    dbWorker.log(joins.length, 'joins:', joinClause);

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
    // eslint-disable-next-line no-console
    console.error('SQL returned:', {
      conditions, group_by, order_by, limit,
    }, sql);
    return sql.trim();
  }

  return toSql();
};
Worker.prototype.buildSqlFromQuery.metadata = {
  bot: true,
  options: {},
};

module.exports = Worker;
