const util = require('util');
const debug = require('debug')('SegmentWorker');
// const debugMore = require('debug')('debug:SQLWorker');

const JSON5 = require('json5');// Useful for parsing extended JSON

const SQLWorker = require('./SQLWorker');

require('dotenv').config({ path: '.env' });

function Worker(worker) {
  SQLWorker.call(this, worker);
  this.accountId = worker.accountId;
  if (!this.accountId) throw new Error('No accountId provided to SegmentWorker constructor');
  if (worker.knex) {
    this.knex = worker.knex;
  } else {
    this.auth = {
      ...worker.auth,
    };
  }
}

util.inherits(Worker, SQLWorker);
Worker.metadata = {
  alias: 'query',
};

/*
build an array of rules
{"combinator":"and",
    "rules":[
      {"field":"given_name","operator":"=","valueSource":"value","value":"Bob"},
      {"field":"family_name","operator":"beginsWith","valueSource":"value","value":"J"},
      { "combinator":"and","not":false
        "rules":[
          {"field":"age","operator":"=","valueSource":"value","value":42}],
      }
    ]
  }
*/
/*
export const defaultOperators = [
  { name: `=`, value: `=`, label: `=` } as const,
  { name: '!=', value: '!=', label: '!=' } as const,
  { name: '<', value: '<', label: '<' } as const,
  { name: '>', value: '>', label: '>' } as const,
  { name: '<=', value: '<=', label: '<=' } as const,
  { name: '>=', value: '>=', label: '>=' } as const,
  { name: 'contains', value: 'contains', label: 'contains' } as const,
  { name: 'beginsWith', value: 'beginsWith', label: 'begins with' } as const,
  { name: 'endsWith', value: 'endsWith', label: 'ends with' } as const,
  { name: 'doesNotContain', value: 'doesNotContain', label: 'does not contain' } as const,
  { name: 'doesNotBeginWith', value: 'doesNotBeginWith', label: 'does not begin with' } as const,
  { name: 'doesNotEndWith', value: 'doesNotEndWith', label: 'does not end with' } as const,
  { name: 'null', value: 'null', label: 'is null' } as const,
  { name: 'notNull', value: 'notNull', label: 'is not null' } as const,
  { name: 'in', value: 'in', label: 'in' } as const,
  { name: 'notIn', value: 'notIn', label: 'not in' } as const,
  { name: 'between', value: 'between', label: 'between' } as const,
  { name: 'notBetween', value: 'notBetween', label: 'not between' } as const,
]
*/
Worker.prototype.getOperation = function getOperation(operator, source, valueParam) {
  const value = [].concat(valueParam);
  function n(v) {
    return typeof v === 'number' ? v : this.escapeValue(v);
  }
  switch (operator) {
    case '=': return `=${this.escapeValue(value[0])}`;
    case '!=': return `<>${this.escapeValue(value[0])}`;
    case '<': return `<${this.escapeValue(value[0])}`;
    case '>': return `>${this.escapeValue(value[0])}`;
    case '<=': return `<=${this.escapeValue(value[0])}`;
    case '>=': return `>=${this.escapeValue(value[0])}`;
    case 'contains': return ` LIKE ${this.escapeValue(`%${value[0]}%`)}`;
    case 'beginsWith': return ` LIKE ${this.escapeValue(`${value[0]}%`)}`;
    case 'endsWith': return ` LIKE ${this.escapeValue(`%${value[0]}`)}`;
    case 'doesNotContain': return ` NOT LIKE ${this.escapeValue(`%${value[0]}%`)}`;
    case 'doesNotBeginWith': return ` NOT LIKE ${this.escapeValue(`${value[0]}%`)}`;
    case 'doesNotEndWith': return ` NOT LIKE ${this.escapeValue(`%${value[0]}`)}`;
    case 'null': return ' IS NULL';
    case 'notNull': return ' IS NOT NULL';
    case 'in': return ` IN (${value.map(n).join(',')})`;
    case 'notIn': return ` NOT IN (${value.map(n).join(',')})`;
    case 'between': return ` BETWEEN ${n(value[0])} AND ${n(value[1])}`;
    case 'notBetween': return ` NOT BETWEEN ${n(value[0])} AND ${n(value[1])}`;
    default: throw new Error(`Unhandled operation type:${operator}`);
  }
};
Worker.prototype.buildRule = function (rule) {
  if (rule.rules) {
    return `${rule.not ? ' NOT ' : ''}(${rule.rules
      .map((r) => this.buildRule(r))
      .join(rule.combinator === 'or' ? ' OR ' : ' AND ')})`;
  } if (rule.field) {
    return this.escapeColumn(rule.field)
      + this.getOperation(rule.operator, rule.valueSource, rule.value);
  }
  return '1=1';// placeholder truthy check
};

/*
  Get all the configuration for the segment
*/
Worker.prototype.getSegment = async function (options) {
  if (!options.id) return options;
  const { data } = await this.query('select * from segment where id=?', [options.id]);
  if (data.length === 0) throw new Error(`Could not find segment ${options.id}`);
  return data[0];
};

Worker.prototype.buildSQLFromQuery = async function (queryProp) {
  let query = queryProp;
  if (typeof query === 'string') {
    try {
      query = JSON5.parse(query);
    } catch (e) {
      debug(e);
      throw new Error('Badly structured query');
    }
  }
  let sql = null;
  try {
    sql = this.buildRule(query);
  } catch (e) {
    debug(e);
    throw new Error('Badly structured query');
  }
  return sql;
};

/*

build_mechanism:
  query
  scheduled_query
  remote
  remote_count

build_schedule: 'string',
build_status: 'string',
build_status_modified_at: 'modified_at',
last_built: 'datetime',
*/

/*
  Determine how to build a type of segment, and initiate it
*/
Worker.prototype.build = async function (options) {
  const segment = await this.getSegment(options);

  switch (segment.type) {
    case 'remote': return { message: 'No need to build remote' };
    case 'remote_count': return { message: 'No need to build remote' };
    case 'scheduled_query':
      if (segment.build_status === 'built') return { message: 'Already built' };
      break;
    case 'query':
    default:
      break;
  }
  const whereClause = this.buildSQLFromQuery(segment.query);
  await this.query(`update segment set status='building',build_status_modified_at=${this.getNowFunction()} where id=?`, [segment.id]);

  await this.query(`insert ignore into segment_people (segment_id,person_id) select ?,id ${whereClause}`, [segment.id]);

  await this.query(`update segment set status='built',build_status_modified_at=${this.getNowFunction()} where id=?`, [segment.id]);
  return {};
};

Worker.prototype.build.metadata = {
  options: {
    query: {},
  },
};

Worker.prototype.count = async function (queryProp) {
  const whereClause = await this.buildSQLFromQuery(queryProp);
  const sql = `select count(*) as people from person WHERE ${whereClause} limit 10`;
  const { data } = await this.query(sql);
  return data[0];
};

Worker.prototype.count.metadata = {
  options: {
    query: {},
  },
};

/*
Worker.prototype.getSampleFields = async function () {

  this.describe(`person_email`

};
*/

Worker.prototype.sample = async function (queryProp) {
  const whereClause = await this.buildSQLFromQuery(queryProp);
  const sql = `select id,given_name,family_name from person WHERE ${whereClause} limit 10`;
  const { data } = await this.query(sql);
  return { data };
};
Worker.prototype.sample.metadata = {
  options: {
    query: {},
  },
};

Worker.prototype.stats = async function (queryProp) {
  const whereClause = await this.buildSQLFromQuery(queryProp);
  const sql = `select count(*) as records from person WHERE ${whereClause}`;
  const { data } = await this.query(sql);
  return { data };
};

Worker.prototype.stats.metadata = {
  options: {
    query: {},
  },
};

module.exports = Worker;
