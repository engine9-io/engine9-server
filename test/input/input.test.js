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
const InputWorker = require('../../workers/InputWorker');
require('../test_db_schema');
const { createActionFile } = require('../sample_data/generate_fake_data');

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
  });

  it('Should be able to add standard identifiers to an array', async () => {
    const batch = [
      {
        remote_id: '5bbbc3a9-ee40-41b8-b243-1176007346fb',
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
        remote_id: '7a23d9fc-5f53-4c30-89ca-07a79d7af682',
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

    await inputWorker.appendPersonId({ batch });
    batch.forEach((o) => {
      debug(`${o.email}->${o.person_id}`);
      assert.ok(o.person_id > 0, `No valid person information for ${o.email}`);
    });

    await inputWorker.appendEntryId({ pluginId, batch });
    debug(batch);
    batch.forEach((o) => {
      assert.ok(o.id?.length > 0, `Not a valid entry.id: ${o.id}`);
      const ts = new Date(o.ts).getTime();
      const entryts = getUUIDTimestamp(o.id).getTime();
      assert.ok(ts === entryts, 'Timestamps for entry_id (a v7 uuid) and ts don\'t match');
    });
  });

  it('Should be able to append identifiers to a file, and write a sqlite database', async () => {
    const filename = await createActionFile();
    const output = await inputWorker.load({ pluginId: 'testing', filename });
    debug('Input', filename);
    debug('Output filename', output);
  });
});