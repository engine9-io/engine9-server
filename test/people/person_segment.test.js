const {
  describe, it, after,
} = require('node:test');
const debug = require('debug')('person_segment.test.js');
const assert = require('node:assert');
require('../test_db_schema');

const PersonWorker = require('../../workers/PersonWorker');
const SchemaWorker = require('../../workers/SchemaWorker');
const SegmentWorker = require('../../workers/SegmentWorker');
const WorkerRunner = require('../../scheduler/WorkerRunner');

describe('Deploy schemas,upsert people,test segments', async () => {
  const accountId = 'test';

  const runner = new WorkerRunner();
  const env = runner.getWorkerEnvironment({ accountId });
  if (!env) throw new Error(`Could not find enviroment for account ${accountId}`);
  const personWorker = new PersonWorker(env);
  const knex = await personWorker.connect();
  debug('Completed connecting to database');
  const segmentWorker = new SegmentWorker({ ...env, knex });
  const schemaWorker = new SchemaWorker({ ...env, knex });

  after(async () => {
    debug('Destroying knex');
    await knex.destroy();
    // await schemaWorker.destroy();
    // await personWorker.destroy();
    // await segmentWorker.destroy();
  });

  it('should have deployed person/person_identifier/person_email/person_address/person_phone/transaction', async () => {
    const { tables } = await schemaWorker.tables();
    const missing = ['person',
      'person_identifier',
      'person_email',
      'person_address',
      'person_phone',
      'transaction'].filter((d) => !tables.find((t) => t === d));
    assert.equal(missing.length, 0, `Missing tables ${missing.join()}`);
  });

  it('Should be able to upsert and deduplicate people and email addresses', async () => {
    const length = 500;
    const batch = [...new Array(500)].map((x, i) => ({ email: `test${i % (length / 2)}@test.com` }));
    await schemaWorker.query('delete from person_email');
    await personWorker.loadPeople({
      stream: JSON.parse(JSON.stringify(batch)),
      inputId: process.env.testingInputId,
    });
    await personWorker.loadPeople({
      stream: JSON.parse(JSON.stringify(batch)),
      inputId: process.env.testingInputId,
    });
    await personWorker.loadPeople({
      stream: JSON.parse(JSON.stringify(batch)),
      inputId: process.env.testingInputId,
    });
    const { data } = await schemaWorker.query('select count(*) as records from person_email');
    assert.deepEqual(data[0].records, length / 2, 'Does not match');
    debug('Finished up');
  });
  /*
  it('should build a segment from a query object', async () => {
    const sql = await segmentWorker.buildSQLFromQuery({
      query: {
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
      },
    });
    debug('Results of ', sql);
    const expected = "from person where
    (`given_name`='Bob' AND `given_name` LIKE 'B%' AND
      NOT (`family_name`='Jane' OR `family_name` LIKE 'J%'))";
    debug({ sql, expected });
    assert.equal(sql, expected, "SQL from build does't match expectations");
  });
  */

  it('should build a segment that counts donors', async () => {
    const sql = await segmentWorker.buildSQLFromQuery({
      query: {
        rules: [
          {
            field: 'given_name', operator: '=', valueSource: 'value', value: 'Bob',
          },
        ],
      },
    });

    const { data } = await schemaWorker.query(`select count(*)${sql}`);
    debug(data);
  });

  /*
  it('should return a count of people that matches the database count', async () => {
    const r1 = await segmentWorker.query('select count(*) as records from person');
    const actual = r1?.data?.[0]?.records;

    const r2 = await segmentWorker.stats({});
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
  */
});
