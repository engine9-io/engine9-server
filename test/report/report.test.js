/* eslint-disable camelcase */
const {
  describe, it, after,
} = require('node:test');

process.env.DEBUG = 'report.test.js,SQLWorker,ReportWorker';
const debug = require('debug')('report.test.js');
// const assert = require('node:assert');

// This will configure the .env file when constructing
const WorkerRunner = require('../../scheduler/WorkerRunner');
const ReportWorker = require('../../workers/ReportWorker');

describe('Test Report Builder', async () => {
  const accountId = 'test';
  const runner = new WorkerRunner();
  const env = runner.getWorkerEnvironment({ accountId });
  const reportWorker = new ReportWorker(env);
  after(async () => {
    reportWorker.destroy();
  });

  it('should compile an executable report', async () => {
    const report = {
      description: 'An overview of data in the timeline',
      include_date: true,
      label: 'Person Count By Month',
      template: 'primary',
      components: {
        a_title: 'Count of People by Month Created',
        a1: {
          component: 'FraktureBarChart',
          is_date: true,
          dimension: { fql: 'MONTH(ts)' },
          metrics: [{ label: 'People', fql: 'count(*)' }],
          conditions: [],
        },
      },
      data_sources: {
        default: {
          table: 'person',
          date_field: 'ts',
        },
      },
    };

    debug('Building executable report from ', report);
    const { executableReport } = await reportWorker.compile({ report });
    debug('Resulting executable Report ', executableReport);
  });
});
