const {
  describe, it, before, after,
} = require('node:test');
const debug = require('debug')('insert.test.js');
const assert = require('node:assert');
const { rebuildDB, truncateDB } = require('./test_db_modifications');

const PersonWorker = require('../../workers/PersonWorker');
const SchemaWorker = require('../../workers/SchemaWorker');
const SegmentWorker = require('../../workers/SegmentWorker');
const WorkerRunner = require('../../scheduler/WorkerRunner');

describe('Deploy schemas,upsert people,test segments', async () => {
  const accountId = 'test';

  const runner = new WorkerRunner();
  const env = runner.getWorkerEnvironment({ accountId });
  if (!env) throw new Error(`Could not find enviroment for account ${accountId}`);
  const segmentWorker = new SegmentWorker(env);
  const personWorker = new PersonWorker(env);
  const schemaWorker = new SchemaWorker(env);

  before(async () => {
    if (process.argv.indexOf('rebuild') >= 0) {
      await rebuildDB(env);
    } else if (process.argv.indexOf('truncate') >= 0) {
      await truncateDB(env);
    }
  });

  after(async () => {
    debug('Destroying knex');
    await schemaWorker.destroy();
    await personWorker.destroy();
    await segmentWorker.destroy();
  });

  it('should have deployed person/person_identifier/person_email/person_phone', async () => {
    const { tables } = await schemaWorker.tables();
    const missing = ['person', 'person_identifier', 'person_email', 'person_address', 'person_phone'].filter((d) => !tables.find((t) => t === d));
    assert.equal(missing.length, 0, `Missing tables ${missing.join()}`);
  });

  it('Should be able to upsert and deduplicate people and email addresses', async () => {
    const length = 500;
    const batch = [...new Array(500)].map((x, i) => ({ email: `test${i % (length / 2)}@test.com` }));
    await personWorker.upsertBatch({ batch: JSON.parse(JSON.stringify(batch)) });
    await personWorker.upsertBatch({ batch: JSON.parse(JSON.stringify(batch)) });
    await personWorker.upsertBatch({ batch: JSON.parse(JSON.stringify(batch)) });
    const { data } = await schemaWorker.query('select count(*) as records from person_email');
    assert.deepEqual(data[0].records, length / 2, 'Does not match');
    debug('Finished up');
  });
  const segmentQuery = {
    rules: [
      {
        field: 'given_name', operator: '=', valueSource: 'value', value: 'Bob',
      },
      {
        field: 'given_name', operator: 'beginsWith', valueSource: 'value', value: 'B',
      },
      {
        combinator: 'or',
        not: true,
        rules: [{
          field: 'family_name', operator: '=', valueSource: 'value', value: 'Jane',
        },
        {
          field: 'family_name', operator: 'beginsWith', valueSource: 'value', value: 'J',
        },
        ],
      },
    ],
  };

  it('should build a segment from a query object', async () => {
    const sql = await segmentWorker.build(segmentQuery);
    debug('Results of ', sql);
    const expected = '(given_name=\'Bob\' AND given_name LIKE \'B%\' AND  NOT (family_name=\'Jane\' OR family_name LIKE \'J%\'))';
    assert.equal(sql, expected, "SQL from build does't match expectations");
  });
  it('should return a count of people that matches the database count', async () => {
    const r1 = await segmentWorker.query('select count(*) as records from person');
    const actual = r1?.data?.[0]?.records;

    const r2 = await segmentWorker.count({});
    const segment = r2?.data?.[0]?.records;
    assert.equal(actual, segment, `Actual count of ${JSON.stringify(r1?.data)
    } does not equal segment output: ${JSON.stringify(r2?.data)}`);
    debug(`Actual count of ${JSON.stringify(r1?.data)
    } equals segment output: ${JSON.stringify(r2?.data)}`);
  });

  it('should return 10 sample people that matches the database count', async () => {
    const r1 = await segmentWorker.query('select id from person order by id limit 10');
    const r2 = await segmentWorker.sample({});

    assert.deepEqual(
      r1.data,
      r2.data,
      `Actual sample of ${JSON.stringify(r1?.data)
      } does not equal segment output: ${JSON.stringify(r2?.data)}`,
    );
    debug(`Actual sample of ${JSON.stringify(r1?.data)
    } equals segment output: ${JSON.stringify(r2?.data)}`);
  });
});
