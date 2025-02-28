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

describe('Insert Stream of People, including name updates', async () => {
  const accountId = 'test';
  const runner = new WorkerRunner();
  const env = runner.getWorkerEnvironment({ accountId });
  const sqlWorker = new SQLWorker(env);

  const knex = await sqlWorker.connect();
  const personWorker = new PersonWorker({ accountId, knex });

  after(async () => {
    debug('Destroying knex');
    await knex.destroy();
  });

  it('Should be able to upsert and deduplicate people and email status', async () => {
    await sqlWorker.truncate({ table: 'person' });
    await sqlWorker.truncate({ table: 'person_phone' });
    await sqlWorker.truncate({ table: 'person_identifier' });

    const stream = [
      { given_name: 'Bob', family_name: 'Smith', email: 'x@y.com' },
      { given_name: 'Bob', family_name: 'Smith', email: 'x@y.com' },
      { given_name: 'Jane', family_name: 'Smith', email: 'x2@y2.com' },
    ];

    const loadResults = await personWorker.loadPeople({
      stream,
      inputId: process.env.testingInputId,
    });
    debug('Load results=', loadResults);
    const { data } = await sqlWorker.query('select * from person p join person_email e on (p.id=e.person_id)');
    const emails = data.filter((d) => d.email === 'x@y.com');
    assert.equal(emails.length, 1, `There were ${emails.length} emails, should be 1`);

    const bobs = data.filter((d) => (d.given_name === 'Bob' && d.family_name === 'Smith'));
    const janes = data.filter((d) => (d.given_name === 'Jane' && d.family_name === 'Smith'));
    assert.equal(bobs.length, 1, 'Did not have exactly 1 Bob Smith');
    assert.equal(janes.length, 1, 'Did not have exactly 1 Jane Smith');
    await personWorker.loadPeople({
      stream: [
        { given_name: 'RenamedBob', family_name: 'Smith', email: 'x@y.com' },
      ],
      inputId: process.env.testingInputId,
    });
    const { data: data2 } = await sqlWorker.query('select * from person p join person_email e on (p.id=e.person_id)');
    const bobs2 = data2.filter((d) => (d.given_name === 'RenamedBob' && d.family_name === 'Smith'));
    assert.equal(bobs2.length, 1, 'Renaming issue -- did not have exactly 1 RenamedBob Smith');
  });
});
