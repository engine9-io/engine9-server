const parquet = require('@dsnp/parquetjs');
const { v7: uuidv7 } = require('uuid');
const { getTempFilename } = require('@engine9/packet-tools');

const {
  describe,
} = require('node:test');

describe('Should write a sample file', async () => {
// declare a schema for the `fruits` table
  const schema = new parquet.ParquetSchema({
    uuid: { type: 'BYTE_ARRAY' },
    name: { type: 'UTF8' },
    quantity: { type: 'INT64' },
    price: { type: 'DOUBLE' },
    date: { type: 'TIMESTAMP_MILLIS' },
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
    date: new Date(),
    in_stock: true,
  });
  await writer.appendRow({
    uuid: uuidv7(),
    name: 'oranges',
    quantity: 10,
    price: 2.5,
    date: new Date(),
    in_stock: true,
  });
  await writer.close();
  console.log(`Wrote ${filename}`);
});
