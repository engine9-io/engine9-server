const {
  describe, it, after, before,
} = require('node:test');
const assert = require('node:assert');
const debug = require('debug')('test/sql/sqlite');
const { getTempFilename } = require('@engine9/packet-tools');
const SQLiteWorker = require('../../workers/sql/SQLiteWorker');

const accountId = 'engine9';
const columns = [{
  name: 'id', type: 'id', column_type: 'bigint', unsigned: true, nullable: false, auto_increment: true, knex_method: 'bigIncrements',
},
{
  name: 'sample', type: 'string', column_type: 'varchar', length: 255, knex_args: ((o) => ([o.length || 255])),
},
{
  name: 'sample_2', type: 'string', column_type: 'varchar', length: 255, nullable: false, default_value: 'abc_default', knex_args: ((o) => ([o.length || 255])),
},
];

describe('Create database, insert rows, select rows', async () => {
  const sqliteFile = await getTempFilename({ accountId, postfix: '.sqlite' });
  debug(`Creating new sqlite file:${sqliteFile}`);
  const sqlWorker = new SQLiteWorker({ accountId, sqliteFile });
  const table = 'test_table';
  before(async () => {
    const test = await sqlWorker.query('select 1');
    debug('Test query:', test);
    const test2 = await sqlWorker.knex.raw('drop table if exists foo');
    debug('Test query:', test2);
    const test3 = await sqlWorker.drop({ table });
    debug('Test query 3:', test3);
    await sqlWorker.createTable({
      table,
      columns,
    });
  });
  after(async () => {
    await sqlWorker.drop({ table });
    sqlWorker.destroy();
  });
  it('should be assigned and returned', async () => {
    const array = [
      { id: 1, sample: 'value1' },
      { id: null, sample: 'value2' },
    ];
    await sqlWorker.upsertArray({ table, array });
    const { data } = await sqlWorker.query(`select id,sample from ${table}`);
    array.forEach((d, i) => { d.id = i + 1; });
    assert.deepEqual(data, array);
  });
});

describe('Test multiple parallel files', async () => {
  const table = 'test_table';
  let workers = null;

  before(async () => {
    workers = await Promise.all([1, 2, 3].map(async (i) => {
      const sqliteFile = await getTempFilename({ accountId, postfix: '.sqlite' });
      debug(`Parallel ${i} Creating new sqlite file:${sqliteFile}`);
      return new SQLiteWorker({ accountId, sqliteFile });
    }));
  });

  it('should be able to connect to each', async () => {
    await Promise.all(workers.map(async (sqlWorker) => {
      const { data } = await sqlWorker.query('select 1 as foo');
      assert.deepEqual({ data }, { data: [{ foo: 1 }] });
      return { data };
    }));
    debug('Finshed connection test');
  });
  it('should be able to create tables and upsert to each', async () => {
    const array = [
      { id: 1, sample: 'value1' },
      { id: null, sample: 'value2' },
    ];
    await Promise.all(workers.map(async (sqlWorker) => {
      await sqlWorker.createTable({
        table,
        columns,
      });
      await sqlWorker.upsertArray({ table, array });
      const { data } = await sqlWorker.query(`select id,sample from ${table}`);
      array.forEach((d, i) => { d.id = i + 1; });
      assert.deepEqual(data, array);
      return { data };
    }));
  });
  after(async () => {
    workers.forEach(async (sqlWorker) => {
      // await sqlWorker.drop({ table });
      sqlWorker.destroy();
    });
  });
});
