const {
  describe, it, after,
} = require('node:test');
const assert = require('node:assert');
const { getUUIDTimestamp } = require('@engine9/packet-tools');

process.env.DEBUG = '*';
const debug = require('debug')('test-framework');
// const assert = require('node:assert');
const WorkerRunner = require('../../scheduler/WorkerRunner');
const SQLWorker = require('../../workers/SQLWorker');
const SQLLiteWorker = require('../../workers/sql/SQLiteWorker');
const InputWorker = require('../../workers/InputWorker');
require('../test_db_schema');
const { createSampleActionFile } = require('../sample_data/generate_sample_data');

describe('Insert File of people with options', async () => {
  const accountId = 'engine9';
  const runner = new WorkerRunner();
  const env = runner.getWorkerEnvironment({ accountId });
  debug('Using env:', env);
  const sqlWorker = new SQLWorker(env);

  const knex = await sqlWorker.connect();
  debug('Completed connecting to database');
  const inputWorker = new InputWorker({ accountId, knex });

  after(async () => {
    await knex.destroy();
    await inputWorker.destroy();
  });

  it('Should be able to add standard identifiers to an array', async () => {
    const batch = [
      {
        remote_entry_uuid: '5bbbc3a9-ee40-41b8-b243-1176007346fb',
        ts: '2024-04-27T18:12:36.191Z',
        entry_type: 'FORM_SUBMIT',
        remote_input_id: 'form_0',
        email: 'Margie_Von57@gmail.com',
        remote_input_name: 'Q1 Advocacy Action',
        source_code: 'ACQ_EM_2023_X_123',
        action_target: 'Jasmin.Kovacek-Corkery@hotmail.com',
        action_content: 'Cui optio tamen.',
      },
      {
        remote_entry_uuid: '7a23d9fc-5f53-4c30-89ca-07a79d7af682',
        ts: '2024-08-15T00:37:30.391Z',
        entry_type: 'FORM_SUBMIT',
        remote_input_id: 'form_2',
        email: 'Constantin54@gmail.com',
        remote_input_name: 'Whales are cool',
        source_code: 'V2_ACQ_EM_2023_X_123',
        action_target: 'Jessica_Krajcik13@hotmail.com',
        action_content: 'Stabilis tabgo alo vulticulus deprimo carmen culpo.\n'
          + 'Tabula defungo sunt suppellex colo virtus vinco adnuo incidunt.\n'
          + 'Quos vehemens substantia dolor deduco odio carmen denique.\n'
          + 'Deputo blandior voluptatibus utique.\n'
          + 'Centum veritatis spero corporis cruentus mollitia defleo auditor.',
      },
    ];
    const pluginId = 'testing';

    await inputWorker.appendInputId({ pluginId, batch });
    batch.forEach((o) => {
      assert.ok(o.input_id?.length > 0, `Not a valid input_id ${o.input_id}`);
    });

    await inputWorker.appendEntryTypeId({ batch });
    batch.forEach((o) => {
      assert.ok(o.entry_type_id > 0, `No valid entry_type_id for ${o.entry_type}`);
    });

    await inputWorker.appendSourceCodeId({ batch });
    batch.forEach((o) => {
      assert.ok(o.source_code_id > 0, `No valid source code for ${o.source_code}`);
    });

    await inputWorker.loadPeople({ stream: batch, inputId: process.env.testingInputId });
    batch.forEach((o) => {
      debug(`${o.email}->${o.person_id}`);
      assert.ok(o.person_id > 0, `No valid person information for ${o.email}`);
    });

    // Delete then re-assign the remote_entry_uuid, and check it matches the ts
    batch.forEach((o) => { delete o.remote_entry_uuid; });
    await inputWorker.appendEntryId({ pluginId, batch });
    debug(batch);
    batch.forEach((o) => {
      assert.ok(o.id?.length > 0, `Not a valid entry.id: ${o.id}`);
      const ts = new Date(o.ts).getTime();
      const entryts = getUUIDTimestamp(o.id).getTime();
      assert.ok(ts === entryts, 'Timestamps for entry.id (a v7 uuid) and ts don\'t match');
    });
  });

  it('Should be able to append identifiers to a file', async () => {
    const filename = await createSampleActionFile();
    const output = await inputWorker.id({ pluginId: 'testing', filename });

    debug('Input', filename);
    debug('Output filename', output);
  });

  it('Should be able to append identifiers to a file, and load to the timeline', async () => {
    const filename = await createSampleActionFile();
    const output = await inputWorker.id({ pluginId: 'testing', filename, loadTimeline: true });
    debug('Input', filename);
    debug('Output filename', output);
  });

  it('Should be able to append identifiers to a file, write a sqlite database TWICE, and be deduped', async () => {
    // make sure we use a fresh input
    const rid = `Testing Input ${new Date().toISOString()}`;
    const filename = await createSampleActionFile(
      { ts: new Date().toISOString(), remote_input_id: rid },
    );
    const storedInputInfo = await inputWorker.id({ pluginId: 'testing', filename });

    const sqliteFile = Object.values(storedInputInfo.outputFiles || [])?.[0]?.filename;

    const sqlite = new SQLLiteWorker({ accountId, sqliteFile });
    const { data: initial } = await sqlite.query('select count(*) as records from timeline');
    debug('Initial count', initial);
    await inputWorker.id({ pluginId: 'testing', filename });

    const { data: deduped } = await sqlite.query('select count(*) as records from timeline');
    debug('Subsequent count', deduped);
    assert(initial[0]?.records === deduped[0]?.records, 'Records were not deduplicated');

    const filename2 = await createSampleActionFile(
      { ts: new Date().toISOString(), remote_input_id: rid },
    );
    await inputWorker.id({ pluginId: 'testing', filename: filename2 });
    const { data: expanded } = await sqlite.query('select count(*) as records from timeline');
    assert(expanded[0]?.records > deduped[0]?.records, 'Records were overly deduplicated, there should be more of them');
    sqlite.destroy();
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
