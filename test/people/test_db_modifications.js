const debug = require('debug')('rebuild_db.js');

const SchemaWorker = require('../../workers/SchemaWorker');

async function rebuildDB(opts) {
  const schemaWorker = new SchemaWorker(opts);

  const { tables } = await schemaWorker.tables();
  await Promise.all(tables.map((table) => schemaWorker.drop({ table })));
  const schemas = [
    { schema: '@engine9-interfaces/person' },
    { schema: '@engine9-interfaces/person_email' },
    { schema: '@engine9-interfaces/person_phone' },
    { schema: '@engine9-interfaces/person_address' },
  ];
  debug('Deploying schemas');
  await Promise.all(schemas.map((s) => schemaWorker.deploy(s)));
  debug('Deployed all schemas');
  schemaWorker.destroy();
}
async function truncateDB(opts) {
  const schemaWorker = new SchemaWorker(opts);
  debug('Truncating tables');
  const { tables } = await schemaWorker.tables();
  await Promise.all(tables.map((table) => schemaWorker.truncate({ table })));
  schemaWorker.destroy();
}
module.exports = { rebuildDB, truncateDB };
