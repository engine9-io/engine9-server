/* eslint-disable camelcase */
/* eslint-disable no-await-in-loop */
process.env.DEBUG = '*';
const debug = require('debug')('test-framework');
const assert = require('node:assert');
const {
  describe, it, after, before,
} = require('node:test');

const { fakerEN_US: faker } = require('@faker-js/faker');
const PersonWorker = require('../../workers/PersonWorker');
const SegmentWorker = require('../../workers/SegmentWorker');
const InputWorker = require('../../workers/InputWorker');
const FileWorker = require('../../workers/FileWorker');
const {
  deploy, truncate, insertDefaults,
} = require('../test_db_schema');

describe('Segment works as relates to plugins', async () => {
  const accountId = 'test';

  const segmentWorker = new SegmentWorker({ accountId });

  const knex = await segmentWorker.connect();
  const env = { accountId, knex };
  const { data: [{ connectionTest }] } = await segmentWorker.query('select 1 as connectionTest');
  assert(connectionTest, 1, 'Could not connect to the database');
  debug('Completed connecting to database');
  const personWorker = new PersonWorker(env);
  const inputWorker = new InputWorker(env);

  before(async () => {
    await deploy(env);
    await truncate(env);

    await insertDefaults();
  });

  after(async () => {
    debug('Destroying knex');
    await knex.destroy();
  });

  it('Should be able to add people from a plugin', async () => {
    const stream = [
      { remote_person_id: 1, email: '1@foo.com' },
      { remote_person_id: 2, email: '2@foo.com' },
      { remote_person_id: 3, email: '3@foo.com' },
      { remote_person_id: 4, email: '4@foo.com' },
      { remote_person_id: 10, email: '10@foo.com' },
    ];
    await personWorker.loadPeople({
      stream,
      pluginId: process.env.testingPluginId,
      inputId: process.env.testingInputId,
    });
  });
  it('Should be able to add people from a second plugin and dedupe them', async () => {
    const stream = [
      { remote_person_id: 222, email: '2@foo.com' },
      { remote_person_id: 333, email: '3@foo.com' },
      { remote_person_id: 444, email: '4@foo.com' },
      { remote_person_id: 555, email: '5@foo.com' },
    ];
    await personWorker.loadPeople({
      stream,
      pluginId: process.env.testingPluginIdB,
      inputId: process.env.testingInputIdB,
    });
    const { data: [{ emails }] } = await segmentWorker.query('select count(*) as emails from person_email');
    assert.equal(emails, 6, 'Did not deduplicate on email address');
    const { data: [{ people }] } = await segmentWorker.query('select count(*) as people from person');
    assert.equal(people, 6, 'Did not deduplicate on email address');
  });
  it('Should be able to add people from a timeline input', async () => {
    const stream = [
      { ts: faker.date.past().toISOString(), remote_person_id: 333, entry_type: 'TRANSACTION' },
      { ts: faker.date.past().toISOString(), remote_person_id: 444, entry_type: 'TRANSACTION' },
      { ts: faker.date.past().toISOString(), remote_person_id: 555, entry_type: 'TRANSACTION' },
    ];

    const fworker = new FileWorker(this);
    const { filename } = await fworker.objectStreamToFile({ stream });
    const idFile = await inputWorker.idFiles({
      pluginId: process.env.testingPluginIdB,
      inputId: process.env.testingInputIdB2,
      filename,

    });

    const output = await inputWorker.loadTimelineTables({
      ...idFile,
      loadTimeline: true,
    });
    debug(output);
    const { data: [{ entries }] } = await segmentWorker.query('select count(*) as entries from timeline');
    assert.equal(entries, 3, 'Should have 3 timeline entries');

    const { data: [{ inputs }] } = await segmentWorker.query('select count(*) as inputs from input');
    assert.equal(inputs, 3, 'Should have 3 inputs');
  });

  it('Should be able to join people from all three inputs', async () => {
    const sql = `select count(*) as people from person p
    where 
    (id in (select person_id from 
      input join person_identifier on (input.id=person_identifier.source_input_id)
      where input.plugin_id=${segmentWorker.escapeValue(process.env.testingPluginId)}))
    AND
    (id in (select person_id from 
      input join person_identifier on (input.id=person_identifier.source_input_id)
      where input.plugin_id=${segmentWorker.escapeValue(process.env.testingPluginIdB)}))
    AND
    (id in (select person_id from timeline where input_id=${segmentWorker.escapeValue(process.env.testingInputIdB2)}))`;

    const { data: [{ people }] } = await segmentWorker.query(sql);

    assert.equal(people, 2, `There are ${people} people that are in all three sections, not 2`);
  });

  const includes = [
    {
      expectedResult: 6,
      testingCountColumns: [{ eql: 'count(distinct id)', name: 'people' }],
      columns: [{ eql: 'id', name: 'person_id' }],
      table: 'person',
      conditions: [{
        eql: 'id>0',
      }],
    },
    {
      expectedResult: 5,
      testingCountColumns: [{ eql: 'count(distinct person_identifier.person_id)', name: 'people' }],
      columns: [{ eql: 'person_identifier.person_id', name: 'person_id' }],
      table: 'input',
      joins: [
        {
          table: 'person_identifier',
          join_eql: 'input.id=person_identifier.source_input_id',
        },
      ],
      conditions: [{
        type: 'EQUALS',
        values: [{
          ref: { column: 'plugin_id' },
        }, {
          value: { value: process.env.testingPluginId },
        }],
      }],
    },
    {
      expectedResult: 4,
      testingCountColumns: [{ eql: 'count(distinct person_identifier.person_id)', name: 'people' }],
      columns: [{ eql: 'person_identifier.person_id', name: 'person_id' }],
      table: 'input',
      joins: [
        {
          table: 'person_identifier',
          join_eql: 'input.id=person_identifier.source_input_id',
        },
      ],
      conditions: [{
        type: 'EQUALS',
        values: [{
          ref: { column: 'plugin_id' },
        }, {
          value: { value: process.env.testingPluginIdB },
        }],
      }],
    },
    {
      expectedResult: 3,
      testingCountColumns: [{ eql: 'count(distinct person_id)', name: 'people' }],
      columns: ['person_id'],
      table: 'timeline',
      conditions: [{
        type: 'EQUALS',
        values: [{
          ref: { column: 'input_id' },
        }, {
          value: { value: process.env.testingInputIdB2 },
        }],
      }],
    },
  ];

  it('Should be able build and run partial subqueries from a configuration', async () => {
    // eslint-disable-next-line no-restricted-syntax
    for (const eql of includes) {
      const sql = await segmentWorker.buildSqlFromEQLObject({
        ...eql,
        columns: eql.testingCountColumns,
      });
      const { data: [{ people }] } = await segmentWorker.query(sql);
      assert.equal(people, eql.expectedResult, `Expected ${eql.expectedResult} from query, there are ${people} for query ${sql}`);
    }
  });

  it('Should be able build and run counts from a configuration', async () => {
    const sql = await segmentWorker.getSQL({ includes, count: true });
    const { data: [{ people }] } = await segmentWorker.query(sql);

    assert.equal(people, 2, `Expected 2 people from query, there are ${people} for query ${sql}`);
  });
});
