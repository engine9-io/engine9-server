const {
  describe, it, after,
} = require('node:test');

process.env.DEBUG = '*';
const debug = require('debug')('test_primary_key');
// const assert = require('node:assert');
const WorkerRunner = require('../../scheduler/WorkerRunner');
const SchemaWorker = require('../../workers/SchemaWorker');

describe('Insert File of people with options', async () => {
  const accountId = 'test';
  const runner = new WorkerRunner();
  const env = runner.getWorkerEnvironment({ accountId });
  debug('Using env:', env);
  const schemaWorker = new SchemaWorker(env);
  const table = 'pkey_test';

  const knex = await schemaWorker.connect();
  after(async () => {
    debug('Destroying knex');
    schemaWorker.drop({ table });
    await knex.destroy();
  });

  it('Should create a table with a primary key, then alter a field', async () => {
    await schemaWorker.query(`drop table if exists ${table}`);
    await schemaWorker.query(`create table ${table} (source_code_id int primary key auto_increment)`);
    await schemaWorker.deploy({
      schema: {
        tables: [
          {
            name: table,
            columns: {
              source_code_id: 'id',
            },
          },
        ],
      },
    });
  });
});
