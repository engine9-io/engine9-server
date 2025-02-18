require('dotenv').config({ path: '../.env' });

const {
  describe, it, after, before,
} = require('node:test');
const assert = require('node:assert');
const debug = require('debug')('test/sql/create-and-load');
const SQLWorker = require('../../workers/SQLWorker');

const accountId = 'test';

describe('SQL Create and Load testing', async () => {
  let sqlWorker = null;

  before(async () => {
    sqlWorker = new SQLWorker({ accountId });
  });

  after(async () => {
    await sqlWorker.destroy();
  });

  it('should connect to the database and return a value', async () => {
    const { data } = await sqlWorker.query('select 1 as val');
    assert.equal(data[0]?.val, '1', `Invalid val returned from SQL:${JSON.stringify(data)}`);
  });
  it('should create a table with ints and strings ', async () => {
    const { table } = await sqlWorker.createAndLoadTable({
      stream: [{
        'My UpperCase': 'foo',
        'my.dots.dot': 'dot',
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
        'My UpperCase': 'foo',
        'my.dots.dot': 'dot',
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
    const desc = await sqlWorker.describe({ table });
    const { data } = await sqlWorker.query({ sql: `select * from ${table}` });
    const output = { desc, table, data };
    debug(JSON.stringify(output, null, 4));
    // assert.equal(data[0]?.val, '1', `Invalid val returned`);
  });
});
