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
  const prefix = await inputWorker.getNextTablePrefixCounter();
  const tablePrefix = `testing_${prefix}_`;
  const timelineDetailTable = `${tablePrefix}timeline_sample_details`;

  before(async () => {
    await deploy(env);
    await truncate(env);
    await insertDefaults();
    await sqlWorker.drop({ table: timelineDetailTable });
    await sqlWorker.query('select 1');
    const opts = {
      id: process.env.testingPluginId,
      type: 'local',
      name: 'Sample Timeline Testing',
      path: 'engine9-testing/sql-plugin-timeline',
      tablePrefix,
      schema: {
        tables: [
          {
            name: timelineDetailTable,
            columns: {
              id: 'id_uuid',
              remote_input_id: 'string',
              remote_input_name: 'string',
              email: 'string',
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
    await inputWorker.ensurePlugin(opts);
  });

  after(async () => {
    await knex.destroy();
    await inputWorker.destroy();
  });
  it('Should be able to add standard identifiers to an array', async () => {
    // make sure we use a fresh input
    const fileArray = await Promise.all([0, 1, 2].map(async (i) => {
      const remoteInputId = `Testing Input ${i} ${new Date().toISOString()}`;
      const filename = await createSampleActionFile(
        { ts: new Date().toISOString(), remoteInputId },
      );
      const inputId = await inputWorker.getInputId({
        pluginId: process.env.testingPluginId,
        remoteInputId: `testTimelineEntries${i}`,
      });
      return { filename, inputId };
    }));
    const output = await inputWorker.idAndLoadFiles({
      fileArray,
      loadTimeline: true,
      loadTimelineDetail: true,
      timelineDetailTable,
    });
    const records = output.fileArray.reduce((a, b) => a + b.timelineResults.records, 0);
    const { data } = await sqlWorker.query('select count(*) as records from timeline');
    assert(data[0].records === records, `There were ${data[0].records} found, expected ${records}`);
    // try again, making sure it dedupes
    await inputWorker.idAndLoadFiles({
      fileArray,
      loadTimeline: true,
      loadTimelineDetail: true,
      timelineDetailTable,
    });
    const { data: data2 } = await sqlWorker.query('select count(*) as records from timeline');
    assert(data2[0].records === records, `There were ${data2[0].records} found, expected ${records}`);

    const { data: data3 } = await sqlWorker.query(`select count(*) as records from ${timelineDetailTable}`);
    assert(data3[0].records === records, `There were ${data3[0].records} details records found in table ${timelineDetailTable}, expected ${records}`);

    debug(output);
  });
});
