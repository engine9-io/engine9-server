/* eslint-disable camelcase */
const {
  describe, it, after,
} = require('node:test');

process.env.DEBUG = 'sql.test.js,SQLWorker';
const debug = require('debug')('sql.test.js');
// const assert = require('node:assert');

// This will configure the .env file when constructing
const WorkerRunner = require('../../scheduler/WorkerRunner');
const SQLWorker = require('../../workers/SQLWorker');

describe('Test SQL builder', async () => {
  const accountId = 'test';
  const runner = new WorkerRunner();
  const env = runner.getWorkerEnvironment({ accountId });
  const sqlWorker = new SQLWorker(env);
  after(async () => {
    sqlWorker.destroy();
  });

  it('should build a query from an object with eql', async () => {
    const query = {
      table: 'person',
      columns: [
        // 'id',
        { eql: 'YEAR(modified_at)', alias: 'year_modified' },
        { eql: 'count(id)', alias: 'count' },
      ],
      conditions: [
        { eql: "YEAR(modified_at)>'2020-01-01'" },
        { eql: 'id in (1,2,3)' },
        { eql: 'id in (3)' },
      ],
      groupBy: [{ eql: "YEAR(modified_at)>'2020-01-01'" }],
    };
    debug('Building sql:');
    const sql = await sqlWorker.buildSqlFromEQLObject(query);
    const { data } = await sqlWorker.query(sql);
    debug('Results of ', sql, data);
  });

  it('should build valid sql with a valid in', async () => {
    const obj = {
      table: 'person_email',
      columns: ['*'],
      limit: 25,
      offset: 0,
      conditions: [{ eql: 'id in (1,2,3)' }],
      groupBy: [],
    };
    debug('Building sql:');
    const sql = await sqlWorker.buildSqlFromEQLObject(obj);
    const { data } = await sqlWorker.query(sql);
    debug('Results of ', sql, data);
  });

  it('should build sql joins', async () => {
    const sql = await sqlWorker.buildSqlFromEQLObject({
      table: 'person',
      joins: [
        {
          table: 'person_email',
          join_eql: 'person.id=person_email.person_id',
        },
        {
          table: 'person_email',
          alias: 'work_emails',
          join_eql: "person.id=work_emails.person_id and work_emails.email_type='Work'",
        },
        {
          table: 'person_phone',
          alias: 'phones',
          join_eql: 'person.id=phones.person_id',
        },
      ],
      columns: [
        { eql: 'person.id', alias: 'id' },
        { eql: 'person_email.email', alias: 'email' },
        { eql: 'work_emails.email', alias: 'work_email' },
        { eql: 'phones.phone', alias: 'phone' },
      ],
      conditions: [
        { eql: "YEAR(person.created_at)>'2020-01-01'" },
      ],
      groupBy: [{ eql: 'person.id' }],
      limit: 0,
    });
    debug(typeof sql, '%o', sql);
    await sqlWorker.query(sql);
  });

  // subqueries not working with aliases right now
  it('should build sql with a valid subquery', async () => {
    const sql = await sqlWorker.buildSqlFromEQLObject({
      table: 'subquery_1',
      subquery: {
        table: 'person_email',
        columns: [
          { eql: 'person_id', alias: 'person_id' },
          { eql: 'count(*)', alias: 'emails' },
        ],
        groupBy: [{ eql: 'person_id' }],
        alias: 'subquery_1',
      },
      columns: [
        {
          eql: `case when subquery_1.emails>1
          then 'multi-email' else 'one-email' end`,
          alias: 'level',
        },
        { eql: 'count(person_id)', alias: 'people' },
      ],
      groupBy: [{
        eql: `case when subquery_1.emails>1
        then 'multi-email' else 'one-email' end`,
      }],
    });
    debug(typeof sql, '%o', sql);
  });
});
