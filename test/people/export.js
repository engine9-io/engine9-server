/* eslint-disable camelcase */
/* eslint-disable no-await-in-loop */
process.env.DEBUG = '*';
const assert = require('node:assert');
const {
  describe, it, after, before,
} = require('node:test');
const debug = require('debug')('export test');

const SegmentWorker = require('../../workers/SegmentWorker');
const { rebuildAll } = require('../test_db_schema');

describe('Pushing data out, and ingesting results', async () => {
  const accountId = 'test';
  const segmentWorker = new SegmentWorker({ accountId });

  before(async () => {
    await rebuildAll();
  });

  it('Should be able to turn a configuration into a query', async () => {
    const exportConfig = {
      bindings: {
        /* phones: {
          type: 'sql.query',
          table: 'person_phone',
          lookup: ['person_id'],
           conditions: [
            { eql: `source_input_id='${process.env.testingInputId}'` },
          ],
        },
        */
        remote_ids: {
          type: 'sql.query',
          table: 'person_identifier',
          lookup: ['person_id'],
          conditions: [
            { eql: 'id_type=\'remote_person_id\'' },
          ],
        },
      },
      include: [
        {
          table: 'person_identifier',
          columns: ['person_id'],
          joins: [
            {
              table: 'input',
              join_eql: `source_input_id=input.id AND input.plugin_id='${process.env.testingPluginId}'`,
            },
          ],
          conditions: [
            { eql: 'id_type=\'remote_person_id\'' },
          ],
        },
      ],
      exclude: [
      ],
    };

    const { filename, records, sql } = await segmentWorker.export(exportConfig);
    debug(`Exported ${filename} with sql:`, sql);
    assert(records > 0, 'No records exported');
  });

  after(async () => {
    await segmentWorker.destroy();
  });
});
