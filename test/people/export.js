/* eslint-disable camelcase */
/* eslint-disable no-await-in-loop */
process.env.DEBUG = '*';
const assert = require('node:assert');
const {
  describe, it, after, before,
} = require('node:test');
const debug = require('debug')('export test');

const SegmentWorker = require('../../workers/SegmentWorker');
const FileWorker = require('../../workers/FileWorker');
const { rebuildAll } = require('../test_db_schema');

describe('Pushing data out, and ingesting results', async () => {
  const accountId = 'test';
  const segmentWorker = new SegmentWorker({ accountId });

  before(async () => {
    await rebuildAll();
  });
  let initialFile = null;

  it('Should be able to turn a configuration into an export file', async () => {
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
    initialFile = filename;
  });

  it('Should be able to change data, then export again, and get distinct records', async () => {
    const fworker = new FileWorker(this);
    const { stream } = await fworker.fileToObjectStream({ filename: initialFile });
    const originalData = await stream.toArray();
    assert(originalData.length > 0, 'No records pulled from sample file');

    const sample = originalData[2];
    sample.remote_ids[0].test_value = true;
    // Find the differences in a stream
    const { stream: diffStream } = await fworker.getUniqueStream({
      existingFiles: [initialFile],
      stream: originalData,
    });
    const differences = await diffStream.toArray();

    assert(differences.length === 1, 'There should be only one difference');
    const { stream: diffStream2 } = await fworker.getUniqueStream({
      existingFiles: [initialFile],
      stream: originalData,
      identityFunction: (o) => o.person_id, // there should be no differences
    });
    const differences2 = await diffStream2.toArray();
    assert(differences2.length === 0, 'There should be no differences of person_ids');
  });

  after(async () => {
    await segmentWorker.destroy();
  });
});
