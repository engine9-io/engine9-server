/* eslint-disable camelcase */
const {
  describe, it, after,
} = require('node:test');

process.env.DEBUG = 'query.test.js,QueryWorker';
const debug = require('debug')('query.test.js');
// const assert = require('node:assert');

// This will configure the .env file when constructing
const WorkerRunner = require('../../worker-manager/WorkerRunner');
const QueryWorker = require('../../workers/QueryWorker');

describe('Test Query builder', async () => {
  const accountId = 'test';
  const runner = new WorkerRunner();
  const env = runner.getWorkerEnvironment({ accountId });
  const queryWorker = new QueryWorker(env);
  after(async () => {
    queryWorker.destroy();
  });

  it('should build a query from an object with eql', async () => {
    const query = {
      table: 'person',
      columns: [
        // 'id',
        { eql: 'YEAR(modified_at)', alias: 'year_modified' },
        { eql: 'count(id)', alias: 'count' },
      ],
      conditions: [
        { eql: "YEAR(modified_at)>'2020-01-01'" },
        { eql: 'id in (1,2,3)' },
        { eql: 'id in ( 3 )' },
      ],
      group_by: [{ eql: "YEAR(modified_at)>'2020-01-01'" }],
    };
    debug('Building sql:');
    const sql = await queryWorker.buildSqlFromQueryObject(query);
    const { data } = await queryWorker.query(sql);
    debug('Results of ', sql, data);
  });

  it('should build a valid query with a valid in', async () => {
    const query = {
      table: 'person_email',
      columns: ['*'],
      limit: 25,
      offset: 0,
      conditions: [{ eql: 'id in (1,2,3)' }],
      group_by: [],
    };
    debug('Building sql:');
    const sql = await queryWorker.buildSqlFromQueryObject(query);
    const { data } = await queryWorker.query(sql);
    debug('Results of ', sql, data);
  });

  // subqueries not working with aliases right now
  it('should build a query with a valid subquery', async () => {
    const sql = await queryWorker.buildSqlFromQueryObject({
      table: 'subquery_1',
      subquery: {
        table: 'person_email',
        fields: [
          { eql: 'person_id', alias: 'person_id' },
          { eql: 'count(*)', alias: 'emails' },
        ],
        group_by: [{ eql: 'person_id' }],
        alias: 'subquery_1',
      },
      columns: [
        {
          eql: `case when subquery_1.emails>1
          then 'multi-email' else 'one-email' end`,
          alias: 'level',
        },
        { eql: 'count(person_id)', alias: 'people' },
      ],
      group_by: [{
        eql: `case when subquery_1.emails>1
        then 'multi-email' else 'one-email' end`,
      }],
    });
    debug(typeof sql, '%o', sql);
  });
});
