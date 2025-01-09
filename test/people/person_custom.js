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
  const prefix = await personWorker.getNextTablePrefixCounter();
  const tablePrefix = `person_custom_${prefix}_`;

  after(async () => {
    debug('Destroying knex');
    await knex.destroy();
  });

  it('Should be able to get the next prefix', async () => {
    assert(prefix.match(/^[a-z0-9]+$/), 'Prefix does not match regex');
  });

  it('Should be able to create a plugin with custom fields', async () => {
    const opts = {
      id: pluginId,
      type: 'local',
      name: 'Sample Custom Fields',
      path: '@engine9-interfaces/person_custom',
      tablePrefix,
      schema: {
        tables: [
          {
            name: `${tablePrefix}field`,
            columns: {
              custom_string: 'string',
              custom_int: 'int',
              custom_date: 'date',
              custom_datetime: 'datetime',
            },
          },
        ],
      },
      transforms: {
        inbound: [
          {
            path: 'upsertToTables',

          },
        ],

      },
    };
    await personWorker.ensurePlugin(opts);
    const { data } = await sqlWorker.query(`select * from plugin where table_prefix='${opts.tablePrefix}'`);
    assert(data.length === 1, 'Invalid number of matching plugins');
  });

  it('Should be able to save data to custom fields', async () => {
    const batch = [
      {
        email: 'x@y.com',
        [`${tablePrefix}field.custom_string`]: 'sample',
        [`${tablePrefix}field.custom_int`]: 123,
        [`${tablePrefix}field.custom_date`]: new Date().toISOString().slice(0, 10),
        [`${tablePrefix}field.custom_json`]: { foo: 'bar' },
      },
    ];

    await personWorker.upsertPersonBatch({ batch });
    // const { data } = await sqlWorker.query('select * from ');
  });
});
