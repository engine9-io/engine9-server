/* eslint-disable camelcase */
const {
  describe, it, after,
} = require('node:test');

process.env.DEBUG = 'report.test.js,SQLWorker,ReportWorker';
const debug = require('debug')('report.test.js');
const assert = require('node:assert');

// This will configure the .env file when constructing
const WorkerRunner = require('../../scheduler/WorkerRunner');
const ReportWorker = require('../../workers/ReportWorker');
require('../test_db_schema');

const report = require('../../reports/email_transactions');

describe('Test Report Builder', async () => {
  const accountId = 'test';
  const runner = new WorkerRunner();
  const env = runner.getWorkerEnvironment({ accountId });
  const reportWorker = new ReportWorker(env);
  after(async () => {
    reportWorker.destroy();
  });

  it('should compile an executable report', async () => {
    debug('Building executable report from ', report);
    const { executableReport } = await reportWorker.compileReport({ report });
    debug('Resulting executable Report ', executableReport);
  });
  it('should run an executable report', async () => {
    const r = await reportWorker.runReport({ report });
    assert(!!r, 'No report returned from run');
    // debug(JSON.stringify(r, null, 4));
    assert(Array.isArray(r.components?.a1?.data), `No array returned for values, components=${Object.keys(r.components)}`);
    // assert(r.components?.a1?.data?.[0]?.sleep_1 === 0, 'Incorrect result for sleep test');
  });
});
