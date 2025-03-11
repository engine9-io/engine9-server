/* eslint-disable camelcase */
/* eslint-disable no-await-in-loop */
process.env.DEBUG = '*';
const assert = require('node:assert');
const {
  describe, it, after, before,
} = require('node:test');

const debug = require('debug')('segment/generate-sql.test.js');

const SegmentWorker = require('../../workers/SegmentWorker');
const { rebuildAll } = require('../test_db_schema');

describe('Segment works as relates to plugins', async () => {
  const accountId = 'test';
  const segmentWorker = new SegmentWorker({ accountId });

  before(async () => {
    await rebuildAll();
  });

  after(async () => {
    await segmentWorker.destroy();
  });

  it('Should connect and have some timeline entries', async () => {
    const { data: [{ timeline }] } = await segmentWorker.query('select count(*) as timeline from timeline');

    assert(timeline > 0, 'There are no timeline entries');
  });

  it('Should be able to turn a configuration into a query', async () => {
    const sql = await segmentWorker.getSQL({
      count: true,
      include: [
        {
          table: 'person_identifier',
          columns: ['person_id'],
          conditions: [
            { eql: `source_input_id='${process.env.testingInputId}'` },
          ],
        },
      ],
      exclude: [
        {
          table: 'transaction',
          columns: ['person_id'],
          groupBy: ['person_id'],
          having: [
            { eql: 'sum(amount)<100' },
          ],
        },
      ],
    });
    debug(sql);
    const { data: [{ count }] } = await segmentWorker.query(sql);

    assert(count > 0, 'There should be records');
  });
});
