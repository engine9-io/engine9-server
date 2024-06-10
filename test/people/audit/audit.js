const {
  describe, it, before, after,
} = require('node:test');

process.env.DEBUG = '*';
const debug = require('debug')('test-framework');
const assert = require('node:assert');
const WorkerRunner = require('../../../worker-manager/WorkerRunner');
const SQLWorker = require('../../../workers/SQLWorker');
const PersonWorker = require('../../../workers/PersonWorker');
const { rebuildDB, truncateDB } = require('../test_db_modifications');

describe('Insert File of people with options', async () => {
  const accountId = 'engine9';
  const runner = new WorkerRunner();
  const env = runner.getWorkerEnvironment({ accountId });
  debug('Using env:', env);
  const sqlWorker = new SQLWorker(env);

  const knex = await sqlWorker.connect();
  debug('Completed connecting to database');
  const personWorker = new PersonWorker({ accountId, knex });

  before(async () => {
    if (process.argv.indexOf('rebuild') >= 0) {
      await rebuildDB(env);
    } else if (process.argv.indexOf('truncate') >= 0) {
      await truncateDB(env);
    }
  });

  after(async () => {
    debug('Destroying knex');
    await knex.destroy();
  });

  it('Should be able to upsert and deduplicate people and email status, and produce an audit output', async () => {
    await truncateDB(env);
    debug('Argv=', process.argv);
    const stream = [
      { email: 'x@y.com' },
      { email: 'y@z.com' },
    ];

    await personWorker.upsert({ stream });

    const { data } = await sqlWorker.query('select count(*) as records from person_email');
    debug('Retrieved ', data, ' from database');
    assert.deepEqual(data[0].records, 2, 'Does not match');
    debug('Finished up');
  });
});
