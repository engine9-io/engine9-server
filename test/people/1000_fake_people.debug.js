process.env.DEBUG = '*';
const debug = require('debug')('test-framework');
const assert = require('node:assert');
const WorkerRunner = require('../../scheduler/WorkerRunner');
const SQLWorker = require('../../workers/SQLWorker');
const PersonWorker = require('../../workers/PersonWorker');
const { rebuildDB, truncateDB } = require('../test_db_schema');

(async function () {
  const accountId = 'engine9';
  const runner = new WorkerRunner();
  const env = runner.getWorkerEnvironment({ accountId });
  debug('Using env:', env);
  const sqlWorker = new SQLWorker(env);

  const knex = await sqlWorker.connect();
  debug('Completed connecting to database');
  const personWorker = new PersonWorker({ accountId, knex });

  if (process.argv.indexOf('rebuild') >= 0) {
    await rebuildDB(env);
  } else if (process.argv.indexOf('truncate') >= 0) {
    await truncateDB(env);
  }

  await truncateDB(env);
  debug('Argv=', process.argv);
  let filename = process.argv.pop();
  if (filename.indexOf('.csv') >= 0) {
  // do nothing
  } else {
    filename = `${__dirname}/1000_fake_people.csv.gz`;
  }

  await personWorker.loadPeople({ filename, inputId: process.env.testingInputId });

  const { data } = await sqlWorker.query('select count(*) as records from person_email');
  debug('Retrieved ', data, ' from database');
  assert.deepEqual(data[0].records, 1000, 'Does not match');
  debug('Finished up');
  debug('Destroying knex');
  await knex.destroy();
}());
