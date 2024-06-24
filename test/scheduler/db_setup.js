const debug = require('debug')('rebuild_db.js');

const SchemaWorker = require('../../workers/SchemaWorker');

async function rebuildDB(opts) {
  const schemaWorker = new SchemaWorker(opts);

  await schemaWorker.drop({ table: 'job' });

  const schemas = [
    { schema: '../engine9-interfaces/job/schema.js' },
  ];
  debug('Deploying schemas');
  await Promise.all(schemas.map((s) => schemaWorker.deploy(s)));
  schemaWorker.destroy();
}
async function truncateDB(opts) {
  const schemaWorker = new SchemaWorker(opts);
  debug('Truncating tables');
  await schemaWorker.truncate({ table: 'job' });
  schemaWorker.destroy();
}
module.exports = { rebuildDB, truncateDB };
