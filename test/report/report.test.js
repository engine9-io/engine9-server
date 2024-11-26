/* eslint-disable camelcase */
const {
  describe, it, after,
} = require('node:test');

process.env.DEBUG = 'report.test.js,SQLWorker,ReportWorker';
const debug = require('debug')('report.test.js');
const assert = require('node:assert');
// const sampleReport = require('./sample_report');

// This will configure the .env file when constructing
const WorkerRunner = require('../../scheduler/WorkerRunner');
const ReportWorker = require('../../workers/ReportWorker');
require('../test_db_schema');

describe('Test Report Builder', async () => {
  const accountId = 'test';
  const runner = new WorkerRunner();
  const env = runner.getWorkerEnvironment({ accountId });
  const reportWorker = new ReportWorker(env);
  after(async () => {
    reportWorker.destroy();
  });
  const report = {
    description: 'An overview of data in the timeline',
    include_date: true,
    label: 'Person Count By Month',
    template: 'primary',
    components: {
      a_title: 'Count of People by Month Created',
      a0: {
        component: 'FraktureBarChart',
        is_date: true,
        dimension: { label: 'Month', eql: 'MONTH(created_at)' },
        metrics: [{ label: 'People', eql: 'count(*)' }],
        conditions: [],
      },
      a1: {
        table: 'dual',
        component: 'FraktureBarChart',
        metrics: [{ label: 'sleep_1', eql: 'sleep(1)' }],
      },
      a2: {
        table: 'dual',
        component: 'FraktureBarChart',
        metrics: [{ label: 'sleep_2', eql: 'sleep(1)' }],
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

  it('should compile an executable report', async () => {
    debug('Building executable report from ', report);
    const { executableReport } = await reportWorker.compile({ report });
    debug('Resulting executable Report ', executableReport);
  });
  it('should run an executable report', async () => {
    const r = await reportWorker.run({ report });
    assert(!!r, 'No report returned from run');
    debug(JSON.stringify(r, null, 4));
    assert(Array.isArray(r.components?.a0?.data), 'No array returned for values');
    assert(r.components?.a1?.data?.[0]?.sleep_1 === 0, 'Incorrect result for sleep test');
    debug('Resulting report ', JSON.stringify(r, null, 4));
  });
});
