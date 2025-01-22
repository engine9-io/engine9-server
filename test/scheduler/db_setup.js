const debug = require('debug')('rebuild_db.js');

const SchemaWorker = require('../../workers/SchemaWorker');

async function rebuildDB(opts) {
  const schemaWorker = new SchemaWorker(opts);

  await schemaWorker.drop({ table: 'job' });

  const schemas = [
    { schema: 'engine9-interfaces/job' },
  ];
  debug('Deploying schemas');
  await Promise.all(schemas.map((s) => schemaWorker.deploy(s)));
  schemaWorker.destroy();
}
async function truncateDB(opts) {
  const schemaWorker = new SchemaWorker(opts);
  debug('Truncating tables');
  try {
    await schemaWorker.truncate({ table: 'job' });
  } catch (e) {
    throw new Error('Error truncating test.job, try to rebuild');
  } finally {
    schemaWorker.destroy();
  }
}
module.exports = { rebuildDB, truncateDB };
