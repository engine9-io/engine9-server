process.env.DEBUG = '*';
const parquet = require('@dsnp/parquetjs');
const { v7: uuidv7 } = require('uuid');
const { getTempFilename } = require('@engine9/packet-tools');
const assert = require('node:assert');
const debug = require('debug')('parquet reader test');
const {
  describe, it,
} = require('node:test');
const FileWorker = require('../../workers/FileWorker');

describe('Should write a sample file', async () => {
  it('Should be able to add read both lower case, and optionally mixed case/spaces, from Parquet files', async () => {
    // declare a schema for the `fruits` table
    const schema = new parquet.ParquetSchema({
      uuid: { type: 'BYTE_ARRAY' },
      ts: { type: 'TIMESTAMP_MILLIS' },
      name: { type: 'UTF8' },
      quantity: { type: 'INT64' },
      price: { type: 'DOUBLE' },
      in_stock: { type: 'BOOLEAN' },
      'Upper Case Content': { type: 'UTF8' },
    });
    const filename = await getTempFilename({ postfix: '.parquet' });

    // create new ParquetWriter that writes to 'fruits.parquet`
    const writer = await parquet.ParquetWriter.openFile(schema, filename);

    // append a few rows to the file
    await writer.appendRow({
      uuid: uuidv7(),
      ts: new Date(),
      name: 'apples',
      quantity: 10,
      price: 2.5,
      date: new Date(),
      in_stock: true,
      'Upper Case Content': 'Sample',
    });
    await writer.appendRow({
      uuid: uuidv7(),
      ts: new Date(),
      name: 'oranges',
      quantity: 10,
      price: 2.5,
      date: new Date(),
      in_stock: true,
      'Upper Case Content': 'Sample2',
    });
    await writer.close();
    const fileWorker = new FileWorker(this);
    const { stream } = await fileWorker.fileToObjectStream({
      filename,
      columns: ['uuid', 'upper_case_content'],
    });
    const arr = await stream.toArray();
    const hasUpper = arr.filter((d) => d['Upper Case Content'] !== undefined);
    debug({ arr, hasUpper });
    assert.equal(hasUpper.length, 2, `Should have 2 records with Upper Case Content, found ${hasUpper}`);
  });
});
