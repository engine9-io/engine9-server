const {
  describe, it, after, before,
} = require('node:test');
const assert = require('node:assert');

process.env.DEBUG = '*';
const debug = require('debug')('test-framework');
const { v7: uuidv7 } = require('uuid');
const WorkerRunner = require('../../scheduler/WorkerRunner');
const SQLWorker = require('../../workers/SQLWorker');
const InputWorker = require('../../workers/InputWorker');
const { deploy, truncate, insertDefaults } = require('../test_db_schema');
const { createSampleActionFile } = require('../sample_data/generate_sample_data');

describe('Insert File of people with options', async () => {
  const accountId = 'test';
  const runner = new WorkerRunner();
  const env = runner.getWorkerEnvironment({ accountId });
  const sqlWorker = new SQLWorker(env);

  const knex = await sqlWorker.connect();
  const inputWorker = new InputWorker({ accountId, knex });
  const prefix = await inputWorker.getNextTablePrefixCounter();
  const tablePrefix = `testing_${prefix}_`;

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

  it('Should be able to append identifiers to a file, write to the timeline and supporting tables TWICE , and be deduped', async () => {
    const pluginId = uuidv7();
    const opts = {
      id: pluginId,
      type: 'local',
      name: 'Sample Timeline Testing',
      path: 'engine9-testing/sql-plugin-timeline',
      tablePrefix,
      schema: {
        tables: [
          {
            name: `${tablePrefix}timeline_action`,
            columns: {
              id: 'id_uuid',
              remote_input_id: 'string',
              remote_input_name: 'string',
              email: 'string',
              source_code: 'string',
              action_target: 'string',
              action_content: 'string',
            },
            indexes: [
              { columns: 'id', primary: true },
            ],
          },
        ],
      },
    };
    inputWorker.ensurePlugin(opts);

    // make sure we use a fresh input
    const rid = `Testing Input ${new Date().toISOString()}`;
    const filename = await createSampleActionFile(
      { ts: new Date().toISOString(), remoteInputId: rid },
    );
    const inputId = await inputWorker.getInputId({
      pluginId: process.env.testingPluginId,
      remoteInputId: 'testTimelineEntries',
    });

    const { idFilename: idFile } = await inputWorker.id({ inputId, filename });

    await inputWorker.loadTimeline({ filename: idFile, inputId });
    await inputWorker.loadTimelineDetails({ filename: idFile, table: `${tablePrefix}testing_action_input` });

    const { data: initial } = await sqlWorker.query('select count(*) as records from timeline');
    debug('Initial count', initial);

    await inputWorker.loadTimeline({ filename: idFile, inputId });
    await inputWorker.loadTimelineDetails({ filename: idFile, table: `${tablePrefix}testing_action_input` });

    const { data: deduped } = await sqlWorker.query('select count(*) as records from timeline');
    debug('Subsequent count', deduped);
    assert(initial[0]?.records === deduped[0]?.records, `Records were not deduplicated, initial=${initial[0]?.records}, deduped=${deduped[0]?.records}`);

    const filename2 = await createSampleActionFile(
      { ts: new Date().toISOString(), remoteInputId: rid },
    );

    const { idFilename: idFile2 } = await inputWorker.id({ inputId, filename: filename2 });
    await inputWorker.loadTimeline({ filename: idFile2, inputId });
    await inputWorker.loadTimelineDetails({ filename: idFile2, table: `${tablePrefix}testing_action_input` });

    const { data: expanded } = await sqlWorker.query('select count(*) as records from timeline');
    assert(
      expanded[0]?.records > deduped[0]?.records,
      `Records were overly deduplicated, there should be more of them: initial${deduped[0]?.records} expanded:${expanded[0]?.records}`,
    );

    const { data: pluginCount } = await sqlWorker.query(`select count(*) as records from timeline t join ${tablePrefix}testing_action_input a on (t.id=a.id)`);
    assert(
      expanded[0]?.records === pluginCount[0]?.records,
      `Invalid plugin records count,they should match: timeline: ${expanded[0]?.records} ${tablePrefix}testing_action_input: ${pluginCount[0]?.records}`,
    );
  });
});
