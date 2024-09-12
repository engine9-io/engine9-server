const {
  describe, it, after, before,
} = require('node:test');
const assert = require('node:assert');
const SQLWorker = require('../../workers/SQLWorker');

describe('Upsert ids', async () => {
  const sqlWorker = new SQLWorker({ accountId: 'engine9' });
  const table = 'temp_test_upsert';
  before(async () => {
    await sqlWorker.drop({ table });
    await sqlWorker.createTable({
      table,
      columns: [{
        name: 'id', type: 'id', column_type: 'bigint', unsigned: true, nullable: false, auto_increment: true, knex_method: 'bigIncrements',
      },
      {
        name: 'sample', type: 'string', column_type: 'varchar', length: 255, knex_args: ((o) => ([o.length || 255])),
      },
      {
        name: 'sample_2', type: 'string', column_type: 'varchar', length: 255, nullable: false, default_value: 'abc_default', knex_args: ((o) => ([o.length || 255])),
      },
      ],
    });
  });
  after(async () => {
    await sqlWorker.drop({ table });
    sqlWorker.destroy();
  });
  it('should be assigned and returned', async () => {
    const array = [
      { id: 1, sample: 'value1' },
      { sample: 'value2' },
    ];
    await sqlWorker.upsertArray({ table, array });
    const { data } = await sqlWorker.query(`select id,sample from ${table}`);
    array.forEach((d, i) => { d.id = i + 1; });
    assert.deepEqual(data, array);
  });
});
