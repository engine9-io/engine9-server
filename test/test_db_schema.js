const debug = require('debug')('test_data.js');
const { fakerEN_US: faker } = require('@faker-js/faker');
const fs = require('node:fs');
const { stringify } = require('csv');
const { Readable } = require('node:stream');
const { pipeline } = require('node:stream/promises');
const { getTempFilename } = require('@engine9/packet-tools');
const WorkerRunner = require('../scheduler/WorkerRunner');
const InputWorker = require('../workers/InputWorker');
const SchemaWorker = require('../workers/SchemaWorker');

process.env.testingPluginId = '00000000-aa12-4ca3-8c06-0816736fde0f';// testing ID
process.env.testingInputId = '00000000-f623-11ef-9cd2-0242ac120002';// testing ID
process.env.testingTransactionInputId = '00000000-8c6c-4151-b04f-b8b05a01a989';// testing ID
process.env.testingPluginIdB = '00000000-ff01-42b3-ba2d-a2db610b6450';// testing ID
process.env.testingInputIdB = '00000000-ff99-4697-a508-888de9fceafe';// testing ID
process.env.testingInputIdB2 = '00000000-0898-4891-9d90-e9cb489e4143';// testing ID2

const accountId = 'test';
const runner = new WorkerRunner();
const env = runner.getWorkerEnvironment({ accountId });

let personId = 100000000;
function createRandomPerson() {
  const region = faker.location.state({ abbreviated: true });
  personId += Math.floor(Math.random() * 5);
  return {
    person_id: personId, // userId: faker.string.uuid(),
    // username: faker.internet.userName(),
    given_name: faker.person.firstName(),
    family_name: faker.person.lastName(),
    email: faker.internet.email(),
    phone: faker.phone.number(),
    mobile_phone: faker.phone.number(),
    birthdate: faker.date.birthdate(),
    street_1: faker.location.streetAddress(true),
    street_2: '',
    city: faker.location.city(),
    region,
    postal_code: faker.location.zipCode({ state: region }),
    country: faker.location.countryCode('alpha-2'),
    avatar: faker.image.avatar(),
  };
}

const count = 100;
const recurs = ['', '', '', '', '', '', '', '', '', 'daily', 'weekly', 'monthly', 'monthly', 'monthly', 'monthly', 'monthly', 'quarterly', 'annually', 'annually'];
const formNames = ['Q1 Advocacy Action', 'Q2 Petition', 'Whales are cool'];
const transactionFormNames = ['Donation1', 'Transaction2', 'EOQ Transaction3'];
const sourceCodes = ['ACQ_EM_2023_X_123', 'EM_2023_X_123_A', 'EM_2023_X_123_B'];
const actionTargets = ['Dog Catcher Smith', 'Senator Tim Who', 'Congressman Agi Wannabe'];
function pick(a) {
  return a[Math.floor(Math.random() * a.length)];
}

async function createSampleActionFile(opts = {}) {
  const actionArray = [];
  const { users = 10, ts } = opts || {};
  const userArray = opts.userArray || faker.helpers.multiple(createRandomPerson, { count: users });

  let userCounter = 0;
  // eslint-disable-next-line no-restricted-syntax
  for (const user of userArray) {
    // const user = createRandomPerson();
    userCounter += 1;
    if (userCounter % 5000 === 0) debug(`Created ${userCounter}/${users} random users`);

    const { email } = user;
    const actionCount = Math.random() * 3;
    for (let i = 0; i < actionCount; i += 1) {
      const formId = Math.floor(Math.random() * formNames.length);
      actionArray.push({
        remote_entry_uuid: faker.string.uuid(),
        ts: ts || faker.date.past().toISOString(),
        entry_type: 'FORM_SUBMIT',
        remote_input_id: `form_${formId}`,
        remote_input_name: formNames[formId],
        email,
        source_code: pick(sourceCodes),
        action_target: pick(actionTargets),
        action_content: faker.lorem.lines(),
        'Sample Uppercase Content': faker.lorem.lines(),
      });
    }
  }
  const filename = await getTempFilename({ postfix: '.action.csv' });
  await pipeline(
    Readable.from(actionArray),
    stringify({ header: true }),
    fs.createWriteStream(filename),
  );
  return filename;
}

async function createSampleFiles() {
  const transactionArray = [];

  const userArray = faker.helpers.multiple(createRandomPerson, {
    count,
  });
  let counter = 0;
  userArray.forEach((user) => {
    if (Math.random() > 0.3) return null;
    const { email } = user;
    const transCount = Math.random() * 4;
    for (let i = 0; i < transCount; i += 1) {
      counter += 1;
      const formId = Math.floor(Math.random() * transactionFormNames.length);
      transactionArray.push({
        remote_transaction_id: `trans_id.${counter}`,
        entry_date: faker.date.past().toISOString(),
        entry_type: 'TRANSACTION',
        remote_input_id: `form_${formId}`,
        remote_input_name: transactionFormNames[formId],
        email,
        amount: faker.finance.amount({ max: 200, min: 5 }),
        recurs: pick(recurs),
        source_code: pick(sourceCodes),
      });
    }
    return null;
  });
  const personFilename = await getTempFilename({ postfix: '.person.csv' });
  await pipeline(
    Readable.from(userArray),
    stringify({ header: true }),
    fs.createWriteStream(personFilename),
  );

  const transactionFilename = await getTempFilename({ postfix: '.transaction.csv' });
  await pipeline(
    Readable.from(transactionArray),
    stringify({ header: true }),
    fs.createWriteStream(transactionFilename),
  );
  const actionFilename = await createSampleActionFile({ userArray });
  const output = {
    personFilename,
    transactionFilename,
    actionFilename,
  };
  return output;
}

if (require.main === module) {
  debug(`Creating ${count} fake transactions`);
  (async () => {
    // eslint-disable-next-line no-console
    console.log(await createSampleFiles());
  })();
}

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
  await schemaWorker.deploy({ schema: '@engine9-interfaces/transaction' });

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
  await schemaWorker.insertFromStream({
    table: 'plugin',
    stream: [
      { id: process.env.testingPluginId, path: 'workerbots.DBBot', name: 'Testing Plugin' },
      { id: process.env.testingPluginIdB, path: 'workerbots.DBBot', name: 'Testing Plugin B' },
    ],
  });

  await schemaWorker.insertFromStream({
    table: 'input',
    stream: [
      { id: process.env.testingInputId, plugin_id: process.env.testingPluginId, remote_input_id: 'testing-input' },
      { id: process.env.testingTransactionInputId, plugin_id: process.env.testingPluginId, remote_input_id: 'testing-transactions' },
      { id: process.env.testingInputIdB, plugin_id: process.env.testingPluginIdB, remote_input_id: 'testing-input-b1' },
      { id: process.env.testingInputIdB2, plugin_id: process.env.testingPluginIdB, remote_input_id: 'testing-input-b2' },
    ],
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
  try {
    const knex = await schemaWorker.connect();
    try {
      // This could be tighter to check if things are deployed
      const { data: [{ plugins }] } = await schemaWorker.query('select count(*) as plugins from plugin');
      if (plugins !== 2) {
        throw new Error('Not built');
      }
      return { built: true };
    } catch (e) {
      // not built already
    }
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
    const fileArray = [
      { filename: `${__dirname}/sample_data/person.csv`, defaultEntryType: 'FILE_IMPORT', inputId: process.env.testingInputId },
      { filename: `${__dirname}/sample_data/action.csv`, inputId: process.env.testingInputIdB },
      { filename: `${__dirname}/sample_data/transaction.csv`, inputId: process.env.testingTransactionInputId },
    ];

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
  } finally {
    schemaWorker.destroy();
  }
  return { complete: true };
};

module.exports = {
  run,
  drop,
  deploy,
  truncate,
  insertDefaults,
  rebuildAll,
  pluginA,
  createSampleFiles,
  createSampleActionFile,
};
