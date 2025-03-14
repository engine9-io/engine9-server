process.env.DEBUG = '*';
const fs = require('node:fs');

const fsp = fs.promises;
const JSON5 = require('json5');

const { getTempFilename } = require('@engine9/packet-tools');
const assert = require('node:assert');
const {
  describe, it,
} = require('node:test');
const FileWorker = require('../../workers/FileWorker');

const fileWorker = new FileWorker({ accountId: 'test' });

describe('Should write and read multiple jsonl style files', async () => {
  const data = [
    {
      a: 'string', b: 'string', c: 1, d: 2,
    },
    {
      a: 'string', b: 'string', c: 3, d: 4,
    },
    {
      a: 'string', b: 'string', c: 5, d: 6,
    },
  ];
  it('Should be able to read one object per line files', async () => {
    // declare a schema for the `fruits` table
    const filename = await getTempFilename({ postfix: '.jsonl' });

    await fsp.writeFile(filename, data.map((d) => JSON5.stringify(d)).join('\n'));

    const { stream } = await fileWorker.fileToObjectStream({ filename });
    const arr = await stream.toArray();
    for (let i = 0; i < 3; i += 1) {
      assert.deepEqual(arr[i], data[i], `Item ${i} does not match${JSON.stringify(arr[i])}!=${JSON.stringify(data[i])}`);
    }
  });

  it('Should be able to write and read one object per line files', async () => {
    const { filename } = await fileWorker.objectStreamToFile({ stream: data, targetFormat: 'jsonl' });

    const { stream } = await fileWorker.fileToObjectStream({ filename });
    const arr = await stream.toArray();
    for (let i = 0; i < 3; i += 1) {
      assert.deepEqual(arr[i], data[i], `Item ${i} does not match${JSON.stringify(arr[i])}!=${JSON.stringify(data[i])}`);
    }
  });

  it('Should be able to read a named header jsonl file', async () => {
    // declare a schema for the `fruits` table
    const filename = await getTempFilename({ postfix: '.jsonl' });
    const keys = Object.keys(data[0]);
    const content = [
      JSON5.stringify(keys),
    ].concat(
      data.map((d) => JSON5.stringify(Object.values(d))),
    );
    await fsp.writeFile(filename, content.join('\n'));

    const { stream } = await fileWorker.fileToObjectStream({ filename });
    const arr = await stream.toArray();
    for (let i = 0; i < 3; i += 1) {
      assert.deepEqual(arr[i], data[i], `Item ${i} does not match${JSON.stringify(arr[i])}!=${JSON.stringify(data[i])}`);
    }
  });
});
