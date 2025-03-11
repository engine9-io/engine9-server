const debug = require('debug')('test_db_schema.js');

const { createSampleActionFile } = require('./sample_data/generate_sample_data');
const SchemaWorker = require('../workers/SchemaWorker');
const InputWorker = require('../workers/InputWorker');
const WorkerRunner = require('../scheduler/WorkerRunner');

process.env.testingPluginId = '00000000-aa12-4ca3-8c06-0816736fde0f';// testing ID
process.env.testingInputId = '00000000-f623-11ef-9cd2-0242ac120002';// testing ID
process.env.testingPluginIdB = '00000000-ff01-42b3-ba2d-a2db610b6450';// testing ID
process.env.testingInputIdB = '00000000-ff99-4697-a508-888de9fceafe';// testing ID
process.env.testingInputIdB2 = '00000000-0898-4891-9d90-e9cb489e4143';// testing ID2

const accountId = 'test';
const runner = new WorkerRunner();
const env = runner.getWorkerEnvironment({ accountId });

async function drop(opts) {
  const schemaWorker = new SchemaWorker(opts);

  const { tables } = await schemaWorker.tables();
  await Promise.all(tables.map((table) => schemaWorker.drop({ table })));
  schemaWorker.destroy();
}

async function deploy(opts) {
  const schemaWorker = new SchemaWorker(opts);

  debug('Deploying schemas');
  await schemaWorker.deploy({ schema: '@engine9-interfaces/person' });
  await schemaWorker.deploy({ schema: '@engine9-interfaces/person_email' });
  await schemaWorker.deploy({ schema: '@engine9-interfaces/person_phone' });
  await schemaWorker.deploy({ schema: '@engine9-interfaces/person_address' });
  await schemaWorker.deploy({ schema: '@engine9-interfaces/plugin' });
  await schemaWorker.deploy({ schema: '@engine9-interfaces/timeline' });
  await schemaWorker.deploy({ schema: '@engine9-interfaces/source_code' });

  debug('Deployed all schemas');

  if (!opts?.knex) schemaWorker.destroy();
}
async function truncate(opts) {
  const schemaWorker = new SchemaWorker(opts);
  debug('Truncating tables');
  const { tables } = await schemaWorker.tables({ type: 'table' });
  await Promise.all(tables.map((table) => schemaWorker.truncate({ table })));
  if (!opts?.knex) schemaWorker.destroy();
}

async function insertDefaults(opts) {
  const schemaWorker = new SchemaWorker({ accountId, ...opts });
  await schemaWorker.query({
    sql: 'insert ignore into plugin (id,path,name) values (?,?,?)',
    values: [process.env.testingPluginId, 'workerbots.DBBot', 'Testing Plugin'],
  });
  await schemaWorker.query({
    sql: 'insert ignore into input (id,plugin_id,remote_input_id) values (?,?,?)',
    values: [process.env.testingInputId, process.env.testingPluginId, 'testing-input'],
  });

  if (!opts?.knex) schemaWorker.destroy();
}

async function run() {
  if (process.argv.indexOf('drop') >= 0) {
    await drop(env);
  }
  if (process.argv.indexOf('deploy') >= 0) {
    await deploy(env);
    await insertDefaults(env);
  }
  if (process.argv.indexOf('truncate') >= 0) {
    await truncate(env);
    await truncate(env);
    await insertDefaults(env);
  }
}

const pluginA = {
  id: process.env.testingPluginId,
  type: 'local',
  name: 'Sample Testing Plugin A',
  path: 'engine9-testing/sql-plugin-timeline',
  tablePrefix: 'testing_aaa_',
  schema: {
    tables: [
      {
        name: 'testing_aaa_timeline_sample_details',
        columns: {
          id: 'id_uuid',
          remote_input_id: 'string',
          remote_input_name: 'string',
          email: 'string',
          action_target: 'string',
          action_content: 'string',
          sample_uppercase_content: 'string',
        },
        indexes: [
          { columns: 'id', primary: true },
        ],
      },
    ],
  },
};
const pluginB = {
  id: process.env.testingPluginId,
  type: 'local',
  name: 'Sample Testing Plugin B',
  path: 'engine9-testing/sql-plugin-timeline',
  tablePrefix: 'testing_aab_',
  schema: {
  },
};

const rebuildAll = async function () {
  const schemaWorker = new SchemaWorker(env);
  const knex = await schemaWorker.connect();
  const inputWorker = new InputWorker({ accountId, knex });
  const timelineDetailTable = 'testing_aaa_timeline_sample_details';

  await deploy(env);
  await truncate(env);
  await insertDefaults();
  await schemaWorker.drop({ table: timelineDetailTable });
  await schemaWorker.query('select 1');
  await inputWorker.ensurePlugin(pluginA);
  await inputWorker.ensurePlugin(pluginB);
  // make sure we use a fresh input
  const fileArray = await Promise.all([0, 1, 2].map(async (i) => {
    const remoteInputId = `Testing Input ${i} ${new Date().toISOString()}`;
    const filename = await createSampleActionFile(
      { remoteInputId },
    );
    const inputId = await inputWorker.getInputId({
      pluginId: process.env.testingPluginId,
      remoteInputId: `testTimelineEntries${i}`,
    });
    return { filename, inputId };
  }));
  const files = await inputWorker.idFiles({ fileArray });

  await inputWorker.loadTimelineTables({
    ...files,
    loadTimeline: true,
    loadTimelineDetail: true,
    timelineDetailTable,
  });

  const { data: [{ timeline }] } = await schemaWorker.query('select count(*) as timeline from timeline');
  if (timeline === 0) {
    throw new Error('No records loaded to timeline, issue with loading the test data');
  }

  schemaWorker.destroy();
};

module.exports = {
  run, drop, deploy, truncate, insertDefaults, rebuildAll, pluginA,
};
