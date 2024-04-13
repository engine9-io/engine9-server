/* eslint-disable camelcase */
const {
  describe, it, after,
} = require('node:test');

process.env.DEBUG = 'query.test.js,QueryWorker';
const debug = require('debug')('query.test.js');
// const assert = require('node:assert');

// This will configure the .env file when constructing
const QueryWorker = require('../../workers/QueryWorker');

describe('Test Query builder', async () => {
  const accountId = 'test';
  const queryWorker = new QueryWorker({ accountId });
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
        { eql: "YEAR(modified_ad)>'2020-01-01'" },
      ],
      group_by: [{ eql: "YEAR(modified_at)>'2020-01-01'" }],
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
