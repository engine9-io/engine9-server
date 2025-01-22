const {
  describe, it, before, after,
} = require('node:test');
const {
  setTimeout,
} = require('node:timers/promises');

const debug = require('debug')('insert.test.js');
const assert = require('node:assert');

const { rebuildDB, truncateDB } = require('./db_setup');
const Scheduler = require('../../scheduler/SQLJobScheduler');

describe('Schedule and run jobs', async () => {
  const accountId = 'test';
  const opts = { accountId };
  const scheduler = new Scheduler(opts);
  await scheduler.init();

  before(async () => {
    const { data: [{ database }] } = await scheduler.sqlWorker.query('select database() as database');
    assert.equal(database, 'test', `Not in the correct test database, in ${database}`);
    if (process.argv.indexOf('rebuild') >= 0) {
      await rebuildDB(opts);
    } else {
      await truncateDB(opts);
    }
  });

  after(async () => {
    await scheduler.sqlWorker.knex.destroy();
  });
  it('should insert an echo job and receive a result', async () => {
    await scheduler.addJob(
      {
        account_id: 'test',
        worker_path: 'EchoWorker',
        worker_method: 'echo',
        plugin_id: '00000000-0000-0000-0000-000000000001',
        options: JSON.stringify({ foo: 'bar' }),
      },
    );
    const { data: [{ records }] } = await scheduler.sqlWorker.query('select count(*) as records from job where status=\'pending\'');
    assert.equal(records, 1, 'No matching records in job');
    await scheduler.poll({ repeatMilliseconds: false });
    debug('Poller ran, waiting 2 seconds...');
    await setTimeout(2000, 'ok');
    debug('Finished waiting');
    const { data: [{ completed }] } = await scheduler.sqlWorker.query('select count(*) as completed from job where status=\'complete\'');
    assert.equal(completed, 1, 'Job did not complete');
  });
});
