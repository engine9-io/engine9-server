const {
  describe, it, after, before,
} = require('node:test');

process.env.DEBUG = '*';
const debug = require('debug')('test-framework');
const assert = require('node:assert');
const WorkerRunner = require('../../scheduler/WorkerRunner');
const SQLWorker = require('../../workers/SQLWorker');
const PersonWorker = require('../../workers/PersonWorker');
const {
  run, deploy, truncate, insertDefaults,
} = require('../test_db_schema');

describe('Insert File of people with options', async () => {
  const accountId = 'test';
  const runner = new WorkerRunner();
  const env = runner.getWorkerEnvironment({ accountId });
  debug('Using env:', env);
  const sqlWorker = new SQLWorker(env);

  const knex = await sqlWorker.connect();
  debug('Completed connecting to database');
  const personWorker = new PersonWorker({ accountId, knex });

  before(async () => {
    await run();
    await deploy(env);
    await truncate(env);

    await insertDefaults();
  });

  after(async () => {
    debug('Destroying knex');
    await knex.destroy();
  });

  it('Should be able to upsert and deduplicate people and email status', async () => {
    debug('Argv=', process.argv);
    const stream = [
      { email: 'x@y.com', subscription_status: 'Subscribed', type: 'Work' },
      { email: 'x@y.com', subscription_status: 'Subscribed', type: 'Home' },
      {
        person_id: 1, email: 'dupe_email@y.com', subscription_status: 'Subscribed', email_type: 'Home',
      },
      {
        person_id: 2, email: 'dupe_email@y.com', subscription_status: 'Subscribed', email_type: 'Home',
      },
      { email: 'dupe_email@y.com', subscription_status: 'Unsubscribed', email_type: 'Home' },
    ];

    await personWorker.loadPeople({ stream, inputId: process.env.testingInputId });
    const { data } = await sqlWorker.query('select * from person_email');
    const dupeEmail = data.filter((d) => d.email === 'dupe_email@y.com');
    const exes = data.filter((d) => d.email === 'x@y.com');
    assert.equal(exes.length, 1, `There should only have been one x@y.com, there are ${exes.length}`);
    assert.equal(dupeEmail.length, 2, `There were ${dupeEmail.length} dupe_email@y.com addresses, should be 2 -- did not deduplicate correctly when there was a person_id`);
    assert.equal(data.filter((d) => d.email === 'dupe_email@y.com' && d.subscription_status === 'Unsubscribed').length, 2, 'Did not unsubscribe two email addresses');
    assert(data[0].email_hash_v1.length > 0, 'Did not hash the email');
  });
});
