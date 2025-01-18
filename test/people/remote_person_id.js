const {
  describe, it, after,
} = require('node:test');

process.env.DEBUG = '*';
const debug = require('debug')('test-framework');
const assert = require('node:assert');
const WorkerRunner = require('../../scheduler/WorkerRunner');
const SQLWorker = require('../../workers/SQLWorker');
const PersonWorker = require('../../workers/PersonWorker');
require('../test_db_schema');

describe('Testing remote_person_id deduplication', async () => {
  const accountId = 'test';
  const runner = new WorkerRunner();
  const env = runner.getWorkerEnvironment({ accountId });
  debug('Using env:', env);
  const sqlWorker = new SQLWorker(env);

  const knex = await sqlWorker.connect();
  debug('Completed connecting to database');
  const personWorker = new PersonWorker({ accountId, knex });

  after(async () => {
    debug('Destroying knex');
    await knex.destroy();
  });

  it('Should be able to upsert and deduplicate people with same email but different remote_person_id', async () => {
    const stream = [
      {
        remote_person_id: '123425385',
        email: 'dupe_email@test.com',
      },
      {
        remote_person_id: '123425388',
        email: 'dupe_email@test.com',
      },
      {
        remote_person_id: '123425385',
        email: 'second_email@y.com',
      },
      {
        remote_person_id: '123425390',
        email: 'unique_email@y.com',
      },
    ];

    const values = Object.keys(
      stream.reduce((a, b) => {
        b.plugin_id = 'test-plugin';
        a[`test-plugin.${b.remote_person_id}`] = 1; return a;
      }, {}),
    );

    await sqlWorker.query({ sql: `delete p from person_identifier pi join person p on (pi.person_id=p.id) where id_value in (${values.map(() => '?').join(',')})`, values });
    await sqlWorker.query({ sql: `delete p from person_identifier pi join person_email p on (pi.person_id=pi.person_id) where id_value in (${values.map(() => '?').join(',')})`, values });
    await sqlWorker.query({ sql: `delete from person_identifier where id_value in (${values.map(() => '?').join(',')})`, values });
    const { data } = await sqlWorker.query({ sql: `select count(*) as records from person_identifier where id_value in (${values.map(() => '?').join(',')})`, values });
    assert(data?.[0]?.records === 0, 'Should have deleted sample records');
    await personWorker.loadPeople({ stream, inputId: process.env.testingInputId });
    const { data: data2 } = await sqlWorker.query({ sql: `select count(*) as records from person_identifier where id_value in (${values.map(() => '?').join(',')})`, values });
    const ids = data2?.[0]?.records;
    assert.equal(ids, 3, `Should have created 3 sample ids, instead created ${ids}`);
  });
});
