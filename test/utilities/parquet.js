process.env.DEBUG = '*';
const parquet = require('@dsnp/parquetjs');
const { v7: uuidv7 } = require('uuid');
const { getTempFilename } = require('@engine9/packet-tools');
const debug = require('debug')('parquet test');

const {
  describe,
} = require('node:test');

describe('Should write a sample file then read it', async () => {
// declare a schema for the `fruits` table
  const schema = new parquet.ParquetSchema({
    uuid: { type: 'BYTE_ARRAY' },
    ts: { type: 'TIMESTAMP_MILLIS' },
    name: { type: 'UTF8' },
    quantity: { type: 'INT64' },
    price: { type: 'DOUBLE' },
    in_stock: { type: 'BOOLEAN' },
  });
  const filename = await getTempFilename({ postfix: '.parquet' });

  // create new ParquetWriter that writes to 'fruits.parquet`
  const writer = await parquet.ParquetWriter.openFile(schema, filename);

  // append a few rows to the file
  await writer.appendRow({
    uuid: uuidv7(),
    name: 'apples',
    quantity: 10,
    price: 2.5,
    ts: new Date(),
    in_stock: true,
  });
  await writer.appendRow({
    uuid: uuidv7(),
    name: 'oranges',
    quantity: 10,
    price: 2.5,
    ts: new Date(),
    in_stock: true,
  });
  await writer.close();
  debug(`Wrote ${filename}`);
  const reader = await parquet.ParquetReader.openFile(filename);

  // create a new cursor
  const cursor = reader.getCursor();

  // read all records from the file and print them
  let record = null;
  do {
    // eslint-disable-next-line no-await-in-loop
    record = await cursor.next();
    debug('Read:', record);
  } while (record);
});
