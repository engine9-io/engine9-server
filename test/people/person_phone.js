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
        remote_phone_id: '123425385-16822018411',
        remote_person_id: '123425385',
        date_created: '2024-11-15 20:18:22',
        last_modified: '2024-11-15 20:18:22',
        do_not_call: null,
        phone: '15555676789',
        primary: null,
        phone_type: 'Cell',
        preference_order: null,
        sms_status: 'Unsubscribed',
        sms_deliverability_score: 100,
        call_status: 'Subscribed',
      },
      {
        remote_phone_id: '123425388-13179562126',
        remote_person_id: '123425388',
        date_created: '2024-11-15 20:18:22',
        last_modified: '2024-11-15 20:18:22',
        do_not_call: null,
        phone: '15555676789',
        primary: null,
        phone_type: 'Cell',
        preference_order: null,
        sms_status: 'Subscribed',
        sms_deliverability_score: 80,
        call_status: 'Subscribed',
      },
      {
        remote_phone_id: '123425388-13179562127',
        remote_person_id: '123425388',
        date_created: '2024-11-15 20:18:22',
        last_modified: '2024-11-15 20:18:22',
        do_not_call: null,
        phone: '15555676790',
        primary: null,
        phone_type: 'Cell',
        preference_order: null,
        sms_status: 'Subscribed',
        sms_deliverability_score: 80,
        call_status: 'Subscribed',
      },
      {
        remote_phone_id: '123425390-19173341590',
        remote_person_id: '123425390',
        date_created: '2024-11-15 20:18:22',
        last_modified: '2024-11-15 20:18:22',
        do_not_call: null,
        phone: '15558889999',
        primary: null,
        phone_type: 'Cell',
        preference_order: null,
        sms_status: 'Subscribed',
        sms_deliverability_score: 80,
        call_status: 'Subscribed',
      },
    ];

    await sqlWorker.truncate({ table: 'person' });
    await sqlWorker.truncate({ table: 'person_phone' });
    await sqlWorker.truncate({ table: 'person_identifier' });
    await personWorker.upsert({ stream });
    const { data: person } = await sqlWorker.query('select count(*) as records from person');
    assert.equal(person?.[0].records, 3, `Did not deduplicate on remote_person_id, there are ${person?.[0].records} matching people`);
    const { data } = await sqlWorker.query('select * from person_phone');
    const matchingPhones = data.filter((d) => d.phone === '15555676789').length;
    assert.equal(matchingPhones, 1, `Did not deduplicate on phone, there are ${matchingPhones} matching phones`);
    assert(data[0].phone_hash_v1.length > 0, 'Did not hash the phone');
    const missingPersons = data.filter((d) => !d.person_id);
    assert.ok(missingPersons.length === 0, 'There are phone entries without a person_id');
  });
});
