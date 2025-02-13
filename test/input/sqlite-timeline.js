const {
  describe, it, after, before,
} = require('node:test');
const assert = require('node:assert');
const { getTempFilename } = require('@engine9/packet-tools');

process.env.DEBUG = '*';
const debug = require('debug')('test-framework');
// const assert = require('node:assert');
const WorkerRunner = require('../../scheduler/WorkerRunner');
const SQLWorker = require('../../workers/SQLWorker');
const InputWorker = require('../../workers/InputWorker');

const { insertDefaults } = require('../test_db_schema');
const { createSampleActionFile } = require('../sample_data/generate_sample_data');

describe('Insert File of people with options', async () => {
  const accountId = 'test';
  const runner = new WorkerRunner();
  const env = runner.getWorkerEnvironment({ accountId });
  debug('Using env:', env);
  const sqlWorker = new SQLWorker(env);

  const knex = await sqlWorker.connect();
  debug('Completed connecting to database');
  const inputWorker = new InputWorker({ accountId, knex });
  debug('Completed connecting to database');

  before(async () => {
    await insertDefaults();
  });

  after(async () => {
    await knex.destroy();
    await inputWorker.destroy();
  });

  it('Should be able to append identifiers to a file, write a sqlite database TWICE, and be deduped', async () => {
    // make sure we use a fresh input
    const rid = `Testing Input ${new Date().toISOString()}`;
    const filename = await createSampleActionFile(
      { ts: new Date().toISOString(), remoteInputId: rid },
    );
    const inputId = await inputWorker.getInputId({
      pluginId: process.env.testingPluginId,
      remoteInputId: 'testActionIdentifiers',
    });

    const sqliteFile = await getTempFilename({ accountId, postfix: '.sqlite' });

    const { sqliteWorker } = await inputWorker.getTimelineInputSQLiteDB({
      sqliteFile,
      includeEmailDomain: true,
    });

    const { idFilename: idFile } = await inputWorker.id({ inputId, filename });

    await sqliteWorker.loadTimeline({ filename: idFile });

    const { data: initial } = await sqliteWorker.query('select count(*) as records from timeline');
    debug('Initial count', initial);

    await sqliteWorker.loadTimeline({ filename: idFile });

    const { data: deduped } = await sqliteWorker.query('select count(*) as records from timeline');
    debug('Subsequent count', deduped);
    assert(initial[0]?.records === deduped[0]?.records, `Records were not deduplicated, initial=${initial[0]?.records}, deduped=${deduped[0]?.records}`);

    const filename2 = await createSampleActionFile(
      { ts: new Date().toISOString(), remoteInputId: rid },
    );

    const { idFilename: idFile2 } = await inputWorker.id({ inputId, filename: filename2 });
    await sqliteWorker.loadTimeline({ filename: idFile2 });

    const { data: expanded } = await sqliteWorker.query('select count(*) as records from timeline');
    assert(
      expanded[0]?.records > deduped[0]?.records,
      `Records were overly deduplicated, there should be more of them: initial${deduped[0]?.records} expanded:${expanded[0]?.records}`,
    );
    sqliteWorker.destroy();
  });

  /*
  it(`Scale test -- Should be able to append identifiers to a file,
    write a sqlite database, and load to the timeline for a lot of people`, async () => {
    const filename = await createSampleActionFile({ users: 1000000 });
    debug('Input', filename);
    const output = await inputWorker.id{ pluginId: 'testing', filename, loadTimeline: true });

    debug('Output filename', output);
  });
  */
});
