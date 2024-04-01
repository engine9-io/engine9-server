const {
  describe, it, before, after,
} = require('node:test');

process.env.DEBUG = 'test-framework,BaseWorker';
const debug = require('debug')('test-framework');
const assert = require('node:assert');
const SQLWorker = require('../../workers/SQLWorker');
const PersonWorker = require('../../workers/PersonWorker');
const { rebuildDB, truncateDB } = require('./rebuild_db');

describe('Insert File of people with options', async () => {
  const accountId = 'test';
  const sqlWorker = new SQLWorker({ accountId });
  const knex = await sqlWorker.connect();
  const personWorker = new PersonWorker({ accountId, knex });

  before(async () => {
    if (process.argv.indexOf('rebuild') >= 0) {
      await rebuildDB({ accountId });
    } else if (process.argv.indexOf('truncate') >= 0) {
      await truncateDB({ accountId });
    }
  });

  after(async () => {
    debug('Destroying knex');
    await knex.destroy();
  });

  it('Should be able to upsert and deduplicate people and email addresses from a file', async () => {
    await truncateDB({ accountId });
    debug('Argv=', process.argv);
    let filename = process.argv.pop();
    if (filename.indexOf('.csv') >= 0) {
      // do nothing
    } else {
      filename = `${__dirname}/1000_fake_people.csv.gz`;
    }

    await personWorker.upsert({ filename });

    const { data } = await sqlWorker.query('select count(*) as records from person_email');
    debug('Retrieved ', data, ' from database');
    assert.deepEqual(data[0].records, 990, 'Does not match');
    debug('Finished up');
  });
});
