const {
  describe, it, after,
} = require('node:test');

process.env.DEBUG = '*';
const debug = require('debug')('test_primary_key');
// const assert = require('node:assert');
const WorkerRunner = require('../../scheduler/WorkerRunner');
const SchemaWorker = require('../../workers/SchemaWorker');

describe('Test schema modifications', async () => {
  const accountId = 'test';
  const runner = new WorkerRunner();
  const env = runner.getWorkerEnvironment({ accountId });
  debug('Using env:', env);
  const schemaWorker = new SchemaWorker(env);
  const table = 'pkey_test';

  const knex = await schemaWorker.connect();
  after(async () => {
    debug('Destroying knex');
    await schemaWorker.drop({ table });
    await knex.destroy();
  });

  it('Should create a table with a primary key, then alter that field', async () => {
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
  it('Should create a table with a primary key, then alter a field', async () => {
    const table2 = 'test_alter';
    await schemaWorker.query(`drop table if exists ${table2}`);
    await schemaWorker.query(`CREATE TABLE ${table2} (id varchar(64) NOT NULL DEFAULT '',PRIMARY KEY (id))`);
    await schemaWorker.deploy({
      schema: {
        tables: [
          {
            name: table2,
            columns: {
              schema: 'json',
            },
            indexes: [
              { columns: ['schema'] },
            ],
          },
        ],
      },
    });
  });
  /*
  drop table plugin;
  CREATE TABLE `plugin` (
  `id` varchar(64) NOT NULL DEFAULT '',
  `path` varchar(255) DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `modified_at` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`id`))
;
*/
});
