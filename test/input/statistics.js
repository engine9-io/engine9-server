/* eslint-disable no-await-in-loop */
const {
  describe, it, after, before,
} = require('node:test');
const assert = require('node:assert');

process.env.DEBUG = '*';
const debug = require('debug')('test-framework');

const WorkerRunner = require('../../scheduler/WorkerRunner');
const SQLWorker = require('../../workers/SQLWorker');
const InputWorker = require('../../workers/InputWorker');
const { deploy, truncate, insertDefaults } = require('../test_db_schema');
const { createSampleActionFile } = require('../sample_data/generate_sample_data');

describe('id and load multiple files', async () => {
  const accountId = 'test';
  const runner = new WorkerRunner();
  const env = runner.getWorkerEnvironment({ accountId });
  debug('Using env:', env);
  const sqlWorker = new SQLWorker(env);

  const knex = await sqlWorker.connect();
  debug('Completed connecting to database');
  const inputWorker = new InputWorker({ accountId, knex });

  before(async () => {
    await deploy(env);
    await truncate(env);
    await insertDefaults();
    await sqlWorker.query('select 1');
  });

  after(async () => {
    await knex.destroy();
    await inputWorker.destroy();
  });
  it('Should be able to add standard identifiers to an array', async () => {
    // make sure we use a fresh input
    const rawFiles = await Promise.all([0, 1, 2, 3, 4].map(async (i) => {
      const remoteInputId = `Testing Input ${new Date().toISOString()}`;
      const filename = await createSampleActionFile(
        { remoteInputId },
      );
      const inputId = await inputWorker.getInputId({
        pluginId: process.env.testingPluginId,
        remoteInputId: `testTimelineEntries${i}`,
      });
      return { filename, inputId };
    }));
    const directoryMap = {};

    // eslint-disable-next-line no-restricted-syntax
    for (const o of rawFiles) {
      const { inputId, filename } = o;
      const { idFilename } = await inputWorker.id({ filename, inputId });
      const directory = idFilename.split('/').slice(0, -1).join('/');
      directoryMap[directory] = directoryMap[directory] || { directory, sources: [] };
      directoryMap[directory].sources.push({ idFilename, sourceFile: filename });
    }
    const directoryArray = Object.values(directoryMap);
    const stats = await inputWorker.statistics({
      // directoryArray,
      idFilename: directoryArray[0]?.sources?.[0]?.idFilename,
    });
    const { statistics } = stats;
    const fileRecords = directoryArray[0]?.sources.reduce((a, b) => a + b.records, 0);
    assert.equal(fileRecords, statistics.records, `File records don't match statistics records:${fileRecords}!=${statistics.records}`);

    debug(JSON.stringify(statistics, null, 4));
  });
});
