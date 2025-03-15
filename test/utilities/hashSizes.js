/* eslint-disable no-undef */
const {
  describe, it,
} = require('node:test');
const { v4: uuidv4, parse } = require('uuid');
const { createHash } = require('node:crypto');

process.env.DEBUG = '*';
const debug = require('debug')('hashSizeTest');

describe('Memory test', async () => {
  it('Should generate a million uuids and put them into a set', async () => {
    const counter = 1000000;
    const hash1 = new Set();
    const hash2 = new Set();
    const hash3 = new Set();

    await gc();
    const { heapUsed: mem1 } = process.memoryUsage();

    for (let i = 0; i < counter; i += 1) {
      hash1.add(uuidv4());
    }
    await gc();
    const { heapUsed: mem2 } = process.memoryUsage();

    for (let i = 0; i < counter; i += 1) {
      const v = uuidv4();
      const bytes = parse(v);
      hash2.add(bytes);
    }
    await gc();
    const { heapUsed: mem3 } = process.memoryUsage();

    for (let i = 0; i < counter; i += 1) {
      const v = uuidv4();
      const s = createHash('md5').update(v).digest('hex');
      hash3.add(s);
    }

    await gc();
    const { heapUsed: mem4 } = process.memoryUsage();

    debug('Hash1 size=', mem2 - mem1);
    debug('Hash2 size=', mem3 - mem2);
    debug('Hash3 size=', mem4 - mem3);
  });
});
