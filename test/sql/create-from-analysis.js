require('dotenv').config({ path: '../.env' });

const {
  describe, it, after,
} = require('node:test');
const assert = require('node:assert');
const debug = require('debug')('test/sql/create-from-analysis');
const FileWorker = require('../../workers/FileWorker');
const SQLWorker = require('../../workers/SQLWorker');

const accountId = 'test';

describe('SQL Create and Load testing', async () => {
  const sqlWorker = new SQLWorker({ accountId });
  const fworker = new FileWorker({ accountId });

  after(async () => {
    await sqlWorker.destroy();
  });

  it('should create a table from analyzing a stream of data', async () => {
    const columns = [...Array(100).keys()];
    const stream = [1, 2, 3, 4, 5].map((id) => {
      const o = { id };
      columns.forEach((i) => { o[`col_${i}`] = 'test value'; });
      return o;
    });

    const { filename } = await fworker.objectStreamToFile({ stream });
    const analysis = await fworker.analyze({ filename });
    const { table } = await sqlWorker.createTableFromAnalysis({ analysis });
    const desc = await sqlWorker.describe({ table });
    debug(desc.columns.map((d) => `${d.name} ${d.type}`).join(','));
    assert(desc.columns.length > 10, `Invalid number of columns for table ${table}`);
  });
});
