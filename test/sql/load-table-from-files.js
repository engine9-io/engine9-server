require('dotenv').config({ path: '../.env' });

const {
  describe, it, after, before,
} = require('node:test');
const assert = require('node:assert');
// const debug = require('debug')('test/sql/load-table-from-files');
const FileWorker = require('../../workers/FileWorker');
const SQLWorker = require('../../workers/SQLWorker');

const accountId = 'test';

describe('SQL Create and Load testing', async () => {
  let sqlWorker = null;
  let files = [];

  before(async () => {
    sqlWorker = new SQLWorker({ accountId });
    const fworker = new FileWorker({ accountId });
    const { filename: f1 } = await fworker.objectStreamToFile({
      stream: [
        { a: 1, b: 2, c: 3 }, { a: 2, b: 2, c: 3 }, { a: 3, b: 2, c: 3 }, { a: 4, b: 2, c: 3 },
      ],
    });
    const { filename: f2 } = await fworker.objectStreamToFile({
      stream: [
        { a: 5, b: 2, c: 3 }, { a: 6, b: 2, c: 3 }, { a: 7, b: 2, c: 3 }, { a: 8, b: 2, c: 3 },
      ],
    });
    const { filename: f3 } = await fworker.objectStreamToFile({
      stream: [
        { a: 5, b: 2, c: 3 }, { a: 6, b: 2, c: 3 }, { a: 7, b: 2, c: 3 }, { a: 8, b: 2, c: 3 },
      ],
    });
    files = files.concat([f1, f2, f3]);
  });

  after(async () => {
    await sqlWorker.destroy();
  });

  it('should load them to a temp table', async () => {
    const { table } = await sqlWorker.loadTable({ filename: files[0] });
    await sqlWorker.loadTable({ table, filename: files[1] });
    await sqlWorker.loadTable({ table, filename: files[2] });

    const { data } = await sqlWorker.query(`select count(*) as records from ${table}`);
    const expected = 12;
    assert.equal(data[0]?.records, expected, `Invalid val returned from SQL:${JSON.stringify(data)}, should be ${expected}`);
  });

  it('should load them to a temp table using a progress based method', async () => {
    const { table } = await sqlWorker.loadTableFromFiles({
      fileArray: files.map((filename) => ({ filename, random: Math.random() })),
    });
    const { data } = await sqlWorker.query(`select count(*) as records from ${table}`);
    const expected = 12;
    assert.equal(data[0]?.records, expected, `Invalid val returned from SQL:${JSON.stringify(data)}, should be ${expected}`);
  });
});
