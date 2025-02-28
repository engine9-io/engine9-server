process.env.DEBUG = '*,-knex:*';
const {
  describe, it, after, before,
} = require('node:test');
// const assert = require('node:assert');

const debug = require('debug')('test-framework');
const assert = require('node:assert');
const WorkerRunner = require('../../scheduler/WorkerRunner');
const SchemaWorker = require('../../workers/SchemaWorker');
const PersonWorker = require('../../workers/PersonWorker');
const { truncate, insertDefaults } = require('../test_db_schema');

describe('Add ids', async () => {
  const accountId = 'test';
  const runner = new WorkerRunner();
  const env = runner.getWorkerEnvironment({ accountId });
  debug('Using env:', env);
  const schemaWorker = new SchemaWorker(env);

  const knex = await schemaWorker.connect();
  debug('Completed connecting to database');

  const personWorker = new PersonWorker({ accountId, knex });

  before(async () => {
    await schemaWorker.deploy({ schema: 'engine9-interfaces/transaction' });
    await truncate(env);
    await insertDefaults(env);
  });

  after(async () => {
    await knex.destroy();
    await personWorker.destroy();
  });

  it('Should be able to ingest transactions', async () => {
    const batch = [
      {
        remote_transaction_id: 'abc-1234',
        ts: '2024-04-27',
        amount: 25.25,
        entry_type: 'TRANSACTION',
        remote_input_id: 'transaction_page_1',
        remote_input_name: 'Sample Transaction Page',
        email: 'Margie_Von57@gmail.com',
        source_code: 'ACQ_EM_2023_X_123',
        refund_amount: 10,
        recurs: 'MONTHLY',
        recurring_number: 3,
        transaction_custom_detail_1: 'detail_1',
        transaction_custom_detail_2: 'detail_2',
      },
      {
        remote_transaction_id: 'abc-1235',
        ts: '2024-05-27',
        amount: 25.25,
        entry_type: 'TRANSACTION',
        remote_input_id: 'transaction_page_1',
        remote_input_name: 'Sample Transaction Page',
        email: 'Margie_Von57@gmail.com',
        source_code: 'ACQ_EM_2023_X_123',
        refund_amount: 10,
        recurs: 'MONTHLY',
        recurring_number: 3,
        transaction_custom_detail_1: 'detail_1',
        transaction_custom_detail_2: 'detail_2',
      },
    ];

    await personWorker.loadPeople({
      stream: batch,
      inputId: process.env.testingInputId,
      extraPostIdentityTransforms: [
        { path: 'engine9-interfaces/transaction/transforms/inbound/upsert_tables.js', options: {} },
      ],
    });
    debug(batch);

    const { data: [{ ids }] } = await schemaWorker.query('select count(*) as ids from person_identifier');
    assert.equal(ids, 1, `There were ${ids} person_identifier records, should be 1 -- did not deduplicate correctly using email`);
    const { data: [{ transactions }] } = await schemaWorker.query('select count(*) as transactions from transaction');
    assert.equal(transactions, 2, `There were ${transactions} transactions, should be 2 -- did not upsert to transactions correctly`);
  });
});
