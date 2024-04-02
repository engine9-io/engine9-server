const {
  describe, it, before, after,
} = require('node:test');
const debug = require('debug')('insert.test.js');
const assert = require('node:assert');
const { rebuildDB, truncateDB } = require('./test_db_modifications');
const SQLWorker = require('../../workers/SQLWorker');
const PersonWorker = require('../../workers/PersonWorker');
const SchemaWorker = require('../../workers/SchemaWorker');

describe('Deploy schemas and upsert people', async () => {
  const accountId = 'test';
  const sqlWorker = new SQLWorker({ accountId });
  const knex = await sqlWorker.connect();

  const personWorker = new PersonWorker({ accountId, knex });
  const schemaWorker = new SchemaWorker({ accountId, knex });

  before(async () => {
    if (process.argv.indexOf('rebuild') >= 0) {
      await rebuildDB();
    } else if (process.argv.indexOf('truncate') >= 0) {
      await truncateDB();
    }
  });

  after(async () => {
    debug('Destroying knex');
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
