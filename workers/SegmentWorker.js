/* eslint-disable no-await-in-loop */

const util = require('util');
// const debug = require('debug')('SegmentWorker');
// const debugInfo = require('debug')('info:SegmentWorker');

const JSON5 = require('json5');// Useful for parsing extended JSON

const SQLWorker = require('./SQLWorker');

require('dotenv').config({ path: '.env' });

function Worker(worker) {
  SQLWorker.call(this, worker);
  this.accountId = worker.accountId;
}

util.inherits(Worker, SQLWorker);

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

Worker.prototype.getSQL = async function ({ includes, count = false }) {
  const clauses = [];
  // eslint-disable-next-line no-restricted-syntax
  for (const eql of includes) {
    // Check for a column with person_id
    const personId = eql.columns.find((d) => d === 'person_id' || d.name === 'person_id');
    if (!personId) throw new Error(`Error with a subquery, there is no required person_id column defined for include ${JSON5.stringify(eql)}`);
    const sql = await this.buildSqlFromEQLObject(eql);
    clauses.push(`id in (${sql})`);
  }

  const sql = `select ${count ? 'count(id) as people' : 'id as person_id'} from person 
    ${clauses.length > 0 ? ` WHERE ${clauses.join('\n AND ')}` : ''}`;
  return sql;
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
  const sql = await this.getSQL({ ...segment, count: true });
  const { data } = await this.query({ sql });

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
