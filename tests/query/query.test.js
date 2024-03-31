/* eslint-disable camelcase */
const {
  describe, it,
} = require('node:test');
const debug = require('debug')('query.test.js');
const assert = require('node:assert');
const QueryWorker = require('../../workers/QueryWorker');

describe('Test Query builder', async () => {
  const accountId = 'test';
  const queryWorker = new QueryWorker({ accountId });

  it('should build a query from an object with e9ql', () => {
    const query = {
      table: 'person',
      fields: [
        'person_id',
        { e9ql: 'YEAR(last_modified)', alias: 'year_modified' },
      ],
      conditions: [
        { e9ql: "YEAR(last_modified)>'2020-01-01'" },

      ],
      group_by: [{ e9ql: 'person_id' }],
    };
    const sql = queryWorker.buildSqlFromQuery(query);
    debug(sql);
  });

  it('should build a query with a valid subquery', () => {
    const sql = queryWorker.buildSqlFromQuery({
      table: 'summary_table',
      subquery: {
        table: 'transaction_summary',
        fields: [
          { e9ql: 'person_id_int', alias: 'person_id_int' },
          { e9ql: 'sum(amount)', alias: 'revenue' },
        ],
        group_by: [{ e9ql: 'person_id_int' }],
      },
      fields: [
        { table: 'summary_table', e9ql: "case when summary_table.revenue>100 then 'large' else 'small' end", alias: 'level' },
        { e9ql: 'count(summary_table.person_id_int)', alias: 'people' },
        { e9ql: 'sum(summary_table.revenue)', alias: 'total_revenue' },
      ],
      group_by: [{ e9ql: "case when revenue>100 then 'large' else 'small' end" }],
    });
    debug(sql);
  });
});
