process.env.DEBUG = '*';
const {
  describe, it, after, before,
} = require('node:test');
// const assert = require('node:assert');

const debug = require('debug')('test-framework');
// const assert = require('node:assert');
const WorkerRunner = require('../../scheduler/WorkerRunner');
const SQLWorker = require('../../workers/SQLWorker');
const InputWorker = require('../../workers/InputWorker');
const { insertDefaults } = require('../test_db_schema');
const { createSampleActionFile } = require('../sample_data/generate_sample_data');

describe('Add ids', async () => {
  const accountId = 'test';
  const runner = new WorkerRunner();
  const env = runner.getWorkerEnvironment({ accountId });
  debug('Using env:', env);
  const sqlWorker = new SQLWorker(env);

  const knex = await sqlWorker.connect();
  debug('Completed connecting to database');
  const inputWorker = new InputWorker({ accountId, knex });

  before(async () => {
    await insertDefaults();
  });

  after(async () => {
    await knex.destroy();
    await inputWorker.destroy();
  });

  it('Should be able to append identifiers to a huge file', async () => {
    debug('Generating large file');
    const filename = await createSampleActionFile({
      users: 100000,
    });
    debug('Completed generating large file:', filename);
    const inputId = inputWorker.getInputId({
      pluginId: process.env.testingPluginId,
      remoteInputId: 'testActions',
    });

    const output = await inputWorker.id({ inputId, filename });

    debug('Input', filename);
    debug('Output filename', output);
  });
});
