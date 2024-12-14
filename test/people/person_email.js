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

  it('Should be able to upsert and deduplicate people and email status, and produce an audit output', async () => {
    // await truncateDB(env);
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

    await personWorker.upsert({ stream });
    const { data } = await sqlWorker.query('select * from person_email');
    assert.equal(data.filter((d) => d.email === 'x@y.com').length, 1, 'Did not deduplicate on just email address');
    assert.equal(data.filter((d) => d.email === 'dupe_email@y.com').length, 2, 'Mistakenly deduplicated when there was a person_id');
    assert.equal(data.filter((d) => d.email === 'dupe_email@y.com' && d.subscription_status === 'Unsubscribed').length, 2, 'Did not unsubscribe two email addresses');
    assert(data[0].email_hash_v1.length > 0, 'Did not hash the email');
  });
});
