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
const PersonWorker = require('../../workers/PersonWorker');
require('../test_db_schema');

describe('Insert File of people with options', async () => {
  const accountId = 'engine9';
  const runner = new WorkerRunner();
  const env = runner.getWorkerEnvironment({ accountId });
  debug('Using env:', env);
  const sqlWorker = new SQLWorker(env);

  const knex = await sqlWorker.connect();
  debug('Completed connecting to database');
  const personWorker = new PersonWorker({ accountId, knex });

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
    const { data } = await sqlWorker.query('select id from plugin where name=\'Stub Plugin\'');
    const pluginId = data?.[0]?.id;
    if (!pluginId) throw new Error('Could not find a plugin id with name Stub Plugin');

    await personWorker.appendInputId({ pluginId, batch });
    batch.forEach((o) => {
      assert.ok(o.input_id?.length > 0, `No valid input for ${o.input_id}`);
    });

    await personWorker.appendEntryTypeId({ batch });
    batch.forEach((o) => {
      assert.ok(o.source_code_id > 0, `No valid source code for ${o.source_code}`);
    });

    await personWorker.appendSourceCodeId({ batch });
    batch.forEach((o) => {
      assert.ok(o.source_code_id > 0, `No valid source code for ${o.source_code}`);
    });

    await personWorker.appendPersonId({ batch });
    batch.forEach((o) => {
      debug(`${o.email}->${o.person_id}`);
      assert.ok(o.person_id > 0, `No valid person information for ${o.email}`);
    });

    await personWorker.appendEntryId({ pluginId, batch });
    batch.forEach((o) => {
      assert.ok(o.entry_id?.length > 0, `No valid input for ${o.input_id}`);
      const ts = new Date(o.ts).getTime();
      const entryts = getUUIDTimestamp(o.entry_id).getTime();
      assert.ok(ts === entryts, 'Timestamps for entry_id (a v7 uuid) and ts don\'t match');
    });
  });
});
