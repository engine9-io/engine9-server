const {
  describe, it, after,
} = require('node:test');

process.env.DEBUG = '*';
const debug = require('debug')('test-framework');
const assert = require('node:assert');
const { v7: uuidv7 } = require('uuid');
const WorkerRunner = require('../../scheduler/WorkerRunner');
const SQLWorker = require('../../workers/SQLWorker');
const PersonWorker = require('../../workers/PersonWorker');
require('../test_db_schema');

describe('Insert a stream of people with custom fields', async () => {
  const accountId = 'test';
  const pluginId = uuidv7();
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

  it('Should be able to get the next prefix', async () => {
    const prefix = await personWorker.getNextTablePrefixCounter();
    assert(prefix.match(/^[a-z0-9]+$/), 'Prefix does not match regex');
  });
  /*
  it('Should be able to create a plugin with custom fields', async () => {
    const prefix = await personWorker.getNextTablePrefixCounter();
    const opts = {
      id: pluginId,
      name: 'Sample Custom Fields',
      table_prefix: `person_custom_${prefix}`,
      schema: {
        custom_string: 'string',
        custom_int: 'int',
      },
      transforms: {

      },
    };
    await personWorker.getPlugin(opts);
  });
  */
  /*
  it('Should be able to save data to custom fields', async () => {
    const stream = [
      {
        email: 'x@y.com',
        custom_string: 'sample',
        custom_int: 123,
        custom_date: new Date().toISOString().slice(0, 10),
        custom_json: { foo: 'bar' },
      },
    ];

    await personWorker.upsert({ stream });
    const { data } = await sqlWorker.query('select * from ');
  });
  */
});
