/* eslint-disable camelcase */
const {
  describe, it, after,
} = require('node:test');

process.env.DEBUG = 'query.test.js,SQLWorker';
const debug = require('debug')('query.test.js');

// This will configure the .env file when constructing
const WorkerRunner = require('../../scheduler/WorkerRunner');
const QueryWorker = require('../../workers/QueryWorker');

describe('Test SQL builder', async () => {
  const accountId = 'test';
  const runner = new WorkerRunner();
  const env = runner.getWorkerEnvironment({ accountId });
  const queryWorker = new QueryWorker(env);
  after(async () => {
    queryWorker.destroy();
  });

  it('should build a query from a query object', async () => {
    const query = {
      rules: [
        {
          field: 'given_name', operator: '=', valueSource: 'value', value: 'Bob',
        },
        {
          field: 'given_name', operator: 'beginsWith', valueSource: 'value', value: 'B',
        },
        {
          combinator: 'or',
          not: true,
          rules: [{
            field: 'family_name', operator: '=', valueSource: 'value', value: 'Jane',
          },
          {
            field: 'family_name', operator: 'beginsWith', valueSource: 'value', value: 'J',
          },
          ],
        },

      ],
    };
    debug('Building sql:');
    const sql = await queryWorker.build(query);
    debug('Results of ', sql);
  });
});
