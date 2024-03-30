const {
  describe, it, before, after,
} = require('node:test');
const debug = require('debug')('insert.test.js');
const assert = require('node:assert');
const SQLWorker = require('../../workers/SQLWorker');
const PersonWorker = require('../../workers/PersonWorker');
const SchemaWorker = require('../../workers/SchemaWorker');

describe('Deploy schemas and upsert people', async () => {
  const accountId = 'test';
  const sqlWorker = new SQLWorker({ accountId });
  const knex = await sqlWorker.connect();

  const personWorker = new PersonWorker({ accountId, knex });
  const schemaWorker = new SchemaWorker({ accountId, knex });

  debug('Finished constructors');
  async function rebuildDB() {
    debug('Dropping tables');
    const { tables } = await schemaWorker.tables();
    await Promise.all(tables.map((table) => schemaWorker.drop({ table })));
    const schemas = [
      { schema: 'engine9-interfaces/person' },
      { schema: 'engine9-interfaces/person_email' },
      { schema: 'engine9-interfaces/person_phone' },
      { schema: 'engine9-interfaces/person_address' },
    ];
    debug('Deploying schemas');
    await Promise.all(schemas.map((s) => schemaWorker.deploy(s)));
  }
  async function clearDB() {
    debug('Truncating tables');
    const { tables } = await schemaWorker.tables();
    await Promise.all(tables.map((table) => schemaWorker.truncate({ table })));
  }

  before(clearDB);

  after(async () => {
    debug('Destroying person and schema worker');
    await knex.destroy();
  });
  it('should have deployed person/person_identifiers/person_email/person_phone', async () => {
    const { tables } = await schemaWorker.tables();
    const missing = ['person', 'person_identifiers', 'person_email', 'person_address', 'person_phone'].filter((d) => !tables.find((t) => t === d));
    assert.equal(missing.length, 0, `Missing tables ${missing.join()}`);
  });
  it('Should be able to upsert and deduplicate people and email addresses', async () => {
    const length = 500;
    const batch = [...new Array(500)].map((x, i) => ({ email: `test${i % (length / 2)}@test.com` }));
    await personWorker.upsertBatch({ batch: JSON.parse(JSON.stringify(batch)) });
    await personWorker.upsertBatch({ batch: JSON.parse(JSON.stringify(batch)) });
    await personWorker.upsertBatch({ batch: JSON.parse(JSON.stringify(batch)) });
    const { data } = await schemaWorker.query('select count(*) as records from person_email');
    assert.deepEqual(data[0].records, length / 2, 'Does not match');
    debug('Finished up');
  });
});
