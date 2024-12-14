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

describe('Insert File of people with options', async () => {
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

  it('Should be able to upsert and deduplicate people and phone status, and produce an audit output', async () => {
    const stream = [
      {
        remote_person_id: '123425385',
        date_created: '2024-11-15 20:18:22',
        last_modified: '2024-11-15 20:18:22',
      },
      {
        remote_person_id: '123425388',
        date_created: '2024-11-15 20:18:22',
        last_modified: '2024-11-15 20:18:22',
      },
      {
        remote_person_id: '123425390',
        date_created: '2024-11-15 20:18:22',
        last_modified: '2024-11-15 20:18:22',
      },
    ];

    await sqlWorker.query('delete p from person_identifier pi join person p on (pi.person_id=p.id) where id_value in (\'123425385\',\'123425388\',\'123425390\')');
    await sqlWorker.query('delete from person_identifier where id_value in (\'123425385\',\'123425388\',\'123425390\')');
    const { data } = await sqlWorker.query("select count(*) as records from person_identifier where id_value in ('123425385','123425388','123425390')");
    assert(data?.[0]?.records === 0, 'Should have deleted sample records');
    await personWorker.upsert({ stream });
    const { data: data2 } = await sqlWorker.query("select count(*) as records from person_identifier where id_value in ('123425385','123425388','123425390')");
    assert(data2?.[0]?.records === 3, 'Should have created sample records');
  });
});
