/* eslint-disable camelcase */
const util = require('util');
// const debug = require('debug')('ReportWorker');
// const debugInfo = require('debug')('info:ReportWorker');

// const JSON5 = require('json5');// Useful for parsing extended JSON

/*
To convert from fql
--> replace fql with el
--> replace alias with name
--> replace group_by with groupBy
--> replace order_by with orderBy
*/

const SQLWorker = require('./SQLWorker');
const { relativeDate, isValidDate } = require('../utilities');

require('dotenv').config({ path: '.env' });

function Worker(worker) {
  SQLWorker.call(this, worker);
  this.accountId = worker.accountId;
  if (worker.knex) {
    this.knex = worker.knex;
  } else {
    this.auth = {
      ...worker.auth,
    };
  }
}

util.inherits(Worker, SQLWorker);

Worker.prototype.compile = function compile({ report }) {
  if (typeof report !== 'object') throw new Error('compile requires a report object');

  return report;
};

Worker.prototype.compile.metadata = {
  options: {
    query: {},
  },
};

function getDateConditions(start, end, date_column) {
  const c = [];
  if (start || end) {
    if (date_column) {
      if (start) {
        const s = relativeDate(start);
        if (!isValidDate(s)) throw new Error('Invalid start');
        c.push({ fql: `${date_column}>='${s.toISOString()}'` });
      }
      if (end) {
        const e = relativeDate(end);
        if (!isValidDate(e)) throw new Error('Invalid end');
        c.push({ fql: `${date_column}<'${e.toISOString()}'` });
      }
    }
  }
  return c;
}

Worker.prototype.run = async function run({ report, qs = {} }) {
  if (typeof report !== 'object') throw new Error('run requires a report object');

  // eslint-disable-next-line no-restricted-syntax
  for (const c of Object.values(report.components)) {
    if (typeof c === 'object') {
      const dskey = c.data_source || 'default';
      const data_source = report.data_sources[dskey];
      if (!data_source) {
        throw new Error(`Could not find data source ${dskey}`);
      }
      let { query } = c; // hard coded query, no options
      if (!query) {
        const {
          dimension, dimensions, is_date, breakdown, metric, metrics,
        } = c;
        let { conditions } = c;
        c.table = c.table || data_source.table;

        let columns = [].concat(breakdown || []).concat(metric).concat(metrics || []);
        conditions = (conditions || [])
          .concat(data_source.queryStringConditions || []).concat(data_source.conditions || []);
        conditions = conditions.map((x) => {
          if (typeof x === 'string') return { eql: x };
          return x;
        });

        let groupBy = [].concat(breakdown);
        if (is_date) {
        // Don't predefine the domain -- the data should do that even without filters
        // domain=[relativeDate("-3M").getTime(),relativeDate("now").getTime()];
          const dimension_name = 'dimension_name';
          const { days } = qs;
          let date_group = null;
          if (days > 1200) {
            date_group = { name: dimension_name, eql: `YEAR(${dimension.eql})` };
          } else if (days > 365) {
            date_group = { name: dimension_name, eql: `MONTH(${dimension.eql})` };
          } else if (days > 180) {
            date_group = { name: dimension_name, eql: `WEEK(${dimension.eql})` };
          } else {
            date_group = { name: dimension_name, eql: `DAY(${dimension.eql})` };
          }
          columns.unshift(date_group);
          groupBy.unshift(date_group);
        } else if (dimension) {
          const d = [].concat(dimension).filter(Boolean);
          columns = [].concat(d).concat(columns);
          groupBy = [].concat(d);
        } else if (dimensions) {
          const d = [].concat(dimensions).filter(Boolean);
          d.forEach((x) => {
            x.name = x.name || x.label;
          });
          columns = [].concat(d).concat(columns);
          groupBy = [].concat(d);
        }
        columns = columns.filter(Boolean).map((_f, i) => {
          let f = { ..._f };
          if (typeof f === 'string') f = { column: f, name: f };
          if (!f.name) f.name = f.label || `col${i}`;
          // there are limited columns we can pass because of GraphQL, don't return everything
          return { eql: f.eql, name: f.name, column: f.column };
        });
        groupBy = groupBy.filter(Boolean).map((f, i) => {
          if (!f.name) f.name = f.label || `col${i}`;
          return { eql: f.eql, name: f.name };
        });
        query = {
          table: c.table || data_source.table,
          columns: columns || [],
          conditions: conditions || [],
          groupBy: groupBy || [],
          orderBy: c.orderBy || [],
          limit: parseInt(qs.limit || c.limit || 1000, 10),
          offset: qs.offset || c.offset || 0,
        };
        if (data_source.date_column) {
          query.conditions = query.conditions
            .concat(getDateConditions(qs.start, qs.end, data_source.date_column));
        }
        if (data_source.subquery) {
          query.subquery = data_source.subquery;
          if (data_source.subquery.date_column) {
            query.subquery.conditions = (query.subquery.conditions || [])
              .concat(getDateConditions(qs.start, qs.end, data_source.subquery.date_column));
            delete data_source.subquery.date_column;
          } else {
          // do nothing
          }
        }
      }

      // eslint-disable-next-line no-await-in-loop
      c.sql = await this.buildSqlFromEQLObject(query);
    }
  }
  // run these in parallel
  report.components = await Promise.all(
    Object.entries(report.components).map(async ([k, v]) => {
      if (!v.sql) return [k, v];
      v.data = (await this.query(v.sql)).data;
      return [k, v];
    }),
  ).then(Object.fromEntries);

  return report;
};

Worker.prototype.run.metadata = {
  options: {
    query: {},
  },
};

module.exports = Worker;
