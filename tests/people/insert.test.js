const {
  describe, it, before, after,
} = require('node:test');
const debug = require('debug')('insert.test.js');
const assert = require('node:assert');
const PersonWorker = require('../../workers/PersonWorker');
const SchemaWorker = require('../../workers/SchemaWorker');

describe('Deploy schemas and upsert people', async () => {
  const accountId = 'test';
  const personWorker = new PersonWorker({ accountId });
  const schemaWorker = new SchemaWorker({ accountId });
  debug('Finished constructors');
  async function resetDB() {
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

  before(resetDB);

  after(async () => {
    debug('Destroying person and schema worker');
    await personWorker.destroy();
    await schemaWorker.destroy();
  });
  it('should have deployed person/person_identifiers/person_email/person_phone', async () => {
    const { tables } = await schemaWorker.tables();
    const missing = ['person', 'person_identifiers', 'person_email', 'person_address', 'person_phone'].filter((d) => !tables.find((t) => t === d));
    assert.equal(missing.length, 0, `Missing tables ${missing.join()}`);
  });
  it('Should be able to upsert and deduplicate people', async () => {
    const batch = [
      { email: 'test1@test.com' },
      { email: 'test1@test.com' },
      { email: 'test3@test.com' },
    ];
    await personWorker.upsertBatch({
      batch: JSON.parse(JSON.stringify(batch)),
    });
    const { data } = await schemaWorker.query('select email from person_email');
    assert.deepEqual(data, batch, 'Does not match');
    debug('Finished up');
  });
});
