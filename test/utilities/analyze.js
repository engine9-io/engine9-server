const {
  describe, it,
} = require('node:test');
const debug = require('debug')('test/utilities/analyze');
const assert = require('node:assert');

const analyze = require('../../utilities/analyze');

describe('Test stream analysis', async () => {
  it('Should parse and compare dates', async () => {
    const stream = [
      { a: 'foo', b: 1 },
      { a: 'bar', b: 2 },
      { a: 'test', b: 3 },
    ];
    const analysis = await analyze({ stream });
    const correct = {
    };

    debug(analysis);
    assert.deepEqual(analysis, correct, `Analysis statistics not correct, ${JSON.stringify(analysis)}!= ${JSON.stringify(correct)}`);
  });
});
