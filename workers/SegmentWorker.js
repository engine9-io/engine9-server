const util = require('util');
// const debug = require('debug')('SegmentWorker');
const debugInfo = require('debug')('info:SegmentWorker');

const JSON5 = require('json5');// Useful for parsing extended JSON

const SQLWorker = require('./SQLWorker');

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
  if (options.segment_id) throw new Error('Please specify id, not segment_id');
  if (!options.id) {
    return options;
  }
  const { data } = await this.query({ sql: 'select * from segment where id=?', values: [options.id] });
  if (data.length === 0) throw new Error(`Could not find segment ${options.id}`);
  return data[0];
};

Worker.prototype.buildSQLFromQuery = async function ({ query: queryProp }) {
  if (!queryProp) throw new Error('No query provided to buildSQLFromQuery');
  let query = queryProp;
  if (typeof query === 'string') {
    try {
      query = JSON5.parse(query);
    } catch (e) {
      debugInfo(queryProp, e);
      throw new Error('Query cannot be parsed from segment');
    }
  }
  let conditions = null;
  try {
    conditions = this.buildRule(query);
  } catch (e) {
    debugInfo(e, query);
    throw new Error('Cannot build SQL from query');
  }
  if (!conditions || conditions === '()') conditions = '1=1';

  return `from person where ${conditions}`;
};

/*

build_type:
  query
  scheduled_query
  remote
  remote_count

build_schedule: 'string',
build_status:
  new
  counting
  counted
  building
  built
  loading_statistics
  loaded_statistics

build_status_modified_at: 'modified_at',
last_built: 'datetime',
*/

/*
  Determine how to build a type of segment, and initiate it
*/
Worker.prototype.build = async function (options) {
  const segment = await this.getSegment(options);
  if (!segment.id) throw new Error('SegmentWorker.build requires an id to build');
  const sqlFunctions = this.dialect.supportedFunctions();

  switch (segment.type) {
    case 'remote': return { message: 'No need to build remote' };
    case 'remote_count': return { message: 'No need to build remote' };
    case 'scheduled_query':
      if (segment.build_status === 'built') return { message: 'Already built' };
      break;
    case 'query':
    default:
      if (segment.query === undefined) throw new Error('Invalid segment - type is query, but no query object was defined');
      break;
  }
  const whereClause = await this.buildSQLFromQuery(segment);
  let records = null;

  await this.query({ sql: `update segment set build_status='building',build_status_modified_at=${sqlFunctions.NOW()} where id=?`, values: [segment.id] });

  const o = await this.query({ sql: `insert ignore into person_segment (segment_id,person_id) select ?,id ${whereClause}`, values: [segment.id] });
  records = o.records;
  await this.query({ sql: `update segment set people=?,build_status='built',build_status_modified_at=${sqlFunctions.NOW()} where id=?`, values: [records, segment.id] });

  return {
    id: segment.id,
    count: records,
    build_type: segment.build_type,
    build_status: segment.build_status,
    people: records,
    reported_people: segment.reported_people,
  };
};

Worker.prototype.build.metadata = {
  options: {
    query: {},
  },
};

Worker.prototype.count = async function (options) {
  const segment = await this.getSegment(options);

  // remote counts we don't need to count them
  if (segment.build_type === 'remote_count') {
    return {
      id: segment.id,
      count: segment.reported_people,
      build_type: segment.build_type,
      build_status: segment.build_status,
      people: segment.people,
      reported_people: segment.reported_people,
    };
  }
  // We're already counted, just return that
  if (['built'].indexOf(segment.build_status) >= 0) {
    if (segment.people === null) throw new Error(`Segment ${segment.id} has a build status of ${segment.build_status} but null people`);
    return {
      id: segment.id,
      count: segment.people,
      build_type: segment.build_type,
      build_status: segment.build_status,
      people: segment.people,
      reported_people: segment.reported_people,
      source: 'table',
    };
  }

  // just run a count
  const whereClause = await this.buildSQLFromQuery(segment);
  const { data } = await this.query({ sql: `select count(*) as people ${whereClause}` });

  return {
    id: segment.id,
    count: data[0].people,
    build_type: segment.build_type,
    build_status: 'counted',
    people: data[0].people,
    reported_people: segment.reported_people,
    source: 'recount',
  };
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
  const sql = `select id,given_name,family_name ${whereClause} limit 10`;
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
  const sql = `select count(*) as records WHERE ${whereClause}`;
  const { data } = await this.query(sql);
  return { data };
};

Worker.prototype.stats.metadata = {
  options: {
    query: {},
  },
};

module.exports = Worker;
