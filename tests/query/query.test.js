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
  it('should build a query from an object with e9ql', async () => {
    const query = {
      table: 'person',
      columns: [
        // 'id',
        { e9ql: 'YEAR(last_modified)', alias: 'year_modified' },
        { e9ql: 'count(id)', alias: 'count' },
      ],
      conditions: [
        { e9ql: "YEAR(last_modified)>'2020-01-01'" },

      ],
      group_by: [{ e9ql: "YEAR(last_modified)>'2020-01-01'" }],
    };
    debug('Building sql:');
    const sql = await queryWorker.buildSqlFromQuery(query);
    const { data } = await queryWorker.query(sql);
    debug('Results of ', sql, data);
  });
/*
  //subqueries not working with aliases right now
  it('should build a query with a valid subquery', async () => {
    const sql = await queryWorker.buildSqlFromQuery({
      table: 'person',
      subquery: {
        table: 'person_email',
        fields: [
          { e9ql: 'count(*)', alias: 'emails' },
        ],
        group_by: [{ e9ql: 'person_id' }],
      },
      fields: [
        { table: 'person_email', e9ql: "case when person_email.emails>1 then 'multi-email' else 'one-email' end", alias: 'level' },
        { e9ql: 'count(summary_table.person_id_int)', alias: 'people' },
      ],
      group_by: [{ e9ql: "case when person_email.emails>1 then 'multi-email' else 'one-email' end" }],
    });
    debug(sql);
  });
  */
});
