/* eslint-disable camelcase */
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
  const table = `${tablePrefix}field`;

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
            name: table,
            columns: {
              id: 'id',
              custom_string: 'string',
              custom_int: 'int',
              custom_date: 'date',
              custom_datetime: 'datetime',
              custom_json: 'json',
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
    const d = {
      custom_string: 'sample_string',
      custom_int: 123,
      custom_date: new Date().toISOString().slice(0, 10),
      custom_datetime: `${new Date().toISOString().slice(0, -5)}Z`, // database doesn't store millis
      custom_json: JSON.stringify({ foo: 'bar' }),
    };
    const batch = [
      {
        email: 'x@y.com',
        [`${table}.custom_string`]: d.custom_string,
        [`${table}.custom_int`]: d.custom_int,
        [`${table}.custom_date`]: d.custom_date,
        [`${table}.custom_datetime`]: d.custom_datetime,
        [`${table}.custom_json`]: d.custom_json,
      },
    ];

    await personWorker.upsertPersonBatch({ batch });
    const { data } = await sqlWorker.query(`select * from ${table}`);
    assert(data.length > 0, `No records found in table ${table}`);
    let f = 'custom_string';
    assert.deepEqual(data[0][f], d[f], `Field ${f} input did not match output, table: ${table},input:${d[f]},output:${data[0][f]}`);
    f = 'custom_int';
    assert.deepEqual(data[0][f], d[f], `Field ${f} input did not match output, table: ${table},input:${d[f]},output:${data[0][f]}`);
    f = 'custom_date';
    assert.deepEqual(new Date(data[0][f]), new Date(d[f]), `Field ${f} input did not match output, table: ${table},input:${d[f]},output:${data[0][f]}`);
    f = 'custom_datetime';
    assert.deepEqual(new Date(data[0][f]), new Date(d[f]), `Field ${f} input did not match output, table: ${table},input:${d[f]},output:${data[0][f]}`);
    f = 'custom_json';
    assert.deepEqual(data[0][f], d[f], `Field ${f} input did not match output, table: ${table},input:${d[f]},output:${data[0][f]}`);

    await sqlWorker.drop({ table });
  });
});
