/* eslint-disable no-await-in-loop */
const {
  describe, it, after, before,
} = require('node:test');
const assert = require('node:assert');

process.env.DEBUG = '*';
const debug = require('debug')('test-framework');

const fsp = require('node:fs/promises');
const { mkdirp } = require('mkdirp');
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
  it('Should be able to add standard identifiers to an array, then run stats per input on them', async () => {
    // make sure we use a fresh input
    const rawFiles = await Promise.all([0, 1, 2].map(async () => {
      const remoteInputId = 'test';
      // `testTimelineEntries${i}`;// `Testing Input ${new Date().toISOString()}`;
      const file = await createSampleActionFile(
        { remoteInputId },
      );
      const inputId = await inputWorker.getInputId({
        pluginId: process.env.testingPluginId,
        remoteInputId,
      });
      const parts = file.split('/');
      const f = parts.pop();
      const directory = parts.slice(0, -1).concat('stored_input').concat(inputId);
      await mkdirp(directory.join('/'));
      const filename = directory.concat(f).join('/');
      await fsp.rename(file, filename);

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
    const statsArray = await inputWorker.statistics({
      directoryArray,
      writeStatisticsFile: true,
      // idFilename: directoryArray[0]?.sources?.[0]?.idFilename,
    });
    statsArray.forEach((s) => {
      const { statistics, sources } = s;
      const fileRecords = sources.reduce((a, b) => a + b.records, 0);
      assert.equal(fileRecords, statistics.records, `File records don't match statistics records:${fileRecords}!=${statistics.records}`);
    });

    debug(JSON.stringify(statsArray, null, 4));
  });
});
