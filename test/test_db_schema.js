const debug = require('debug')('test_db_schema.js');

const SchemaWorker = require('../workers/SchemaWorker');

async function drop(opts) {
  const schemaWorker = new SchemaWorker(opts);

  const { tables } = await schemaWorker.tables();
  await Promise.all(tables.map((table) => schemaWorker.drop({ table })));
}

async function deploy(opts) {
  const schemaWorker = new SchemaWorker(opts);

  const schemas = [
    { schema: './engine9-interfaces/person' },
    { schema: './engine9-interfaces/person_email' },
    { schema: './engine9-interfaces/person_phone' },
    { schema: './engine9-interfaces/person_address' },
    { schema: './engine9-interfaces/plugin' },
    { schema: './engine9-interfaces/timeline' },
    { schema: './engine9-interfaces/source_code' },
  ];
  debug('Deploying schemas');
  await Promise.all(schemas.map((s) => schemaWorker.deploy(s)));
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
  await schemaWorker.query('insert into plugin (path,name) values (\'engine9StubPlugin\',\'Stub Plugin\')');
  schemaWorker.destroy();
}
module.exports = {
  drop, deploy, truncate, insertDefaults,
};
