const {
  describe, it, after, before,
} = require('node:test');
const assert = require('node:assert');
const PersonWorker = require('../../workers/PersonWorker');
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
  it('Ids are assigned by email', async () => {
    const knex = await sqlWorker.connect();
    const worker1 = new PersonWorker({ accountId: 'engine9' });
    worker1.knex = knex;

    const batch = [
      { email: 'x@y.com' },
      { email: 'x@y.com' },
      { email: 'y@z.com' },
    ];
    for (let i = 0; i < 300; i += 1) {
      batch.push({ email: `${i % 50}@y.com` });
    }
    batch.forEach((d) => {
      d.identifiers = [{ type: 'email', value: d.email }];
    });
    const out = await worker1.appendPersonIds({ batch });

    knex.destroy();
    assert.ok(out?.[0]?.person_id > 0, 'No valid person_id was assigned');
  });

  it('should be assigned and returned', async () => {
    const array = [
      { sample: 'value1' },
      { sample: 'value2' },
    ];
    await sqlWorker.upsertArray({ table, array });
    const { data } = await sqlWorker.query(`select id,sample from ${table}`);
    array.forEach((d, i) => { d.id = i + 1; });
    assert.deepEqual(data, array);
  });
  it('Can upsert people through the pipeline', async () => {
    const knex = await sqlWorker.connect();
    const personWorker = new PersonWorker({ accountId: 'engine9' });

    const batch = [
      { email: 'x@y.com' },
      { email: 'x@y.com' },
      { email: 'y@z.com' },
    ];

    batch.forEach((d) => {
      d.identifiers = [{ type: 'email', value: d.email }];
    });
    const out = await personWorker.ingestPeople({ batch, doNotInsert: true });

    knex.destroy();
    assert.ok(out?.person_email?.length > 0, 'No person_email records');
  });
});
