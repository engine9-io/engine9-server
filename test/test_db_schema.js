const debug = require('debug')('test_db_schema.js');

const SchemaWorker = require('../workers/SchemaWorker');
const WorkerRunner = require('../scheduler/WorkerRunner');

process.env.testingInputId = '11111111-0000-0000-0000-000000000000';// testing ID

async function drop(opts) {
  const schemaWorker = new SchemaWorker(opts);

  const { tables } = await schemaWorker.tables();
  await Promise.all(tables.map((table) => schemaWorker.drop({ table })));
  schemaWorker.destroy();
}

async function deploy(opts) {
  const schemaWorker = new SchemaWorker(opts);

  debug('Deploying schemas');
  await schemaWorker.deploy({ schema: '@engine9-interfaces/person' });
  await schemaWorker.deploy({ schema: '@engine9-interfaces/person_email' });
  await schemaWorker.deploy({ schema: '@engine9-interfaces/person_phone' });
  await schemaWorker.deploy({ schema: '@engine9-interfaces/person_address' });
  await schemaWorker.deploy({ schema: '@engine9-interfaces/plugin' });
  await schemaWorker.deploy({ schema: '@engine9-interfaces/timeline' });
  await schemaWorker.deploy({ schema: '@engine9-interfaces/source_code' });
  await schemaWorker.deploy({ schema: 'engine9-interfaces/transaction' });
  debug('Deployed all schemas');

  schemaWorker.destroy();
}
async function truncate(opts) {
  const schemaWorker = new SchemaWorker(opts);
  debug('Truncating tables');
  const { tables } = await schemaWorker.tables({ type: 'table' });
  await Promise.all(tables.map((table) => schemaWorker.truncate({ table })));
  schemaWorker.destroy();
}

async function insertDefaults(opts) {
  const schemaWorker = new SchemaWorker(opts);
  await schemaWorker.query({
    sql: 'insert ignore into plugin (id,path,name) values (?,?,?)',
    values: [process.env.testingInputId, 'workerbots.DBBot', 'Testing DBBot Plugin'],
  });

  schemaWorker.destroy();
}

const accountId = 'test';
const runner = new WorkerRunner();
const env = runner.getWorkerEnvironment({ accountId });

async function run() {
  if (process.argv.indexOf('drop') >= 0) {
    await drop(env);
  }
  if (process.argv.indexOf('deploy') >= 0) {
    await deploy(env);
    await insertDefaults(env);
  }
  if (process.argv.indexOf('truncate') >= 0) {
    await truncate(env);
    await truncate(env);
    await insertDefaults(env);
  }
}
run();

module.exports = {
  drop, deploy, truncate, insertDefaults,
};
