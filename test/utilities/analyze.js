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
      records: 3,
      fields: [
        {
          name: 'a',
          type: 'string',
          empty: 0,
          min: 'bar',
          max: 'test',
          min_length: 3,
          max_length: 4,
          distinct: 3,
          sample: [
            'foo',
            'bar',
            'test',
          ],
        },
        {
          name: 'b',
          type: 'int',
          empty: 0,
          min: 1,
          max: 3,
          isNumber: true,
          distinct: 3,
          sample: [
            '1',
            '2',
            '3',
          ],
        },
      ],
    };

    debug(analysis);
    assert.equal(JSON.stringify(analysis), JSON.stringify(correct), `Analysis statistics not correct, ${JSON.stringify(analysis, null, 4)}!= ${JSON.stringify(correct, null, 4)}`);
  });
});
