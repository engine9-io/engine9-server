require('dotenv').config({ path: '../.env' });

const {
  describe, it, after, before,
} = require('node:test');
const assert = require('node:assert');
const debug = require('debug')('test/sql/cickhouse');
const ClickHouseWorker = require('../../workers/sql/ClickHouseWorker');

const accountId = 'test';

describe('Clickhouse testing', async () => {
  let sqlWorker = null;

  before(async () => {
    sqlWorker = new ClickHouseWorker({ accountId });
  });

  after(async () => {
    await sqlWorker.destroy();
  });

  it('should connect to the database and return a value', async () => {
    const { data } = await sqlWorker.query('select 1 as val');
    assert.equal(data[0]?.val, '1', `Invalid val returned from Clickhouse:${JSON.stringify(data)}`);
  });
  it('should create a table with ints and strings ', async () => {
    const { table } = await sqlWorker.createAndLoadTable({
      stream: [{
        my_uint: 3,
        my_smallint: 300,
        my_int: -123456,
        my_float: 123.456,
        my_datetime: new Date(),
        my_date: new Date('2024-01-01'),
        my_date2: '2024-01-01',
        my_string: 'abcdeff',
        my_null: 'not-null',
      },
      {
        my_uint: 123,
        my_smallint: -230,
        my_int: 123,
        my_float: 12345.50,
        my_datetime: new Date(),
        my_date: new Date('2024-01-01'),
        my_date2: '2024-01-01',
        my_string: 'abcdeffavaa',
        my_null: null,
      },
      ],
    });
    const desc = await sqlWorker.describe({ table, raw: true });
    const records = await sqlWorker.query({ sql: `select * from ${table}` });
    const output = { desc, table, records };
    debug(output);
    // assert.equal(data[0]?.val, '1', `Invalid val returned`);
  });
});
