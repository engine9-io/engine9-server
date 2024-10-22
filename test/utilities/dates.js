const {
  describe, it,
} = require('node:test');
const debug = require('debug')('test/utilities/date');
const assert = require('node:assert');

const { parseDate } = require('../../utilities');

describe('Test date parsing', async () => {
  it('Should parse and compare dates', async () => {
    const tests = [
      { input: '1924', correct: new Date('1924-01-01').toISOString() },
      { input: '2024', correct: new Date('2024-01-01').toISOString() },
      { input: '02/24', correct: new Date('2024-02-01').toISOString() },
      { input: '02/2024', correct: new Date('2024-02-01').toISOString() },
      { input: '02/01/2024', correct: new Date('2024-02-01').toISOString() },
      { input: '2024-02-01', correct: new Date('2024-02-01').toISOString() },
      { input: '2024-02', correct: new Date('2024-02-01').toISOString() },
      { input: '2024-2', correct: new Date('2024-02-01').toISOString() },
      { input: '5/1/1970', correct: new Date('1970-05-01').toISOString() },
      { input: '2/1964', correct: new Date('1964-02-01').toISOString() },
    ];
    tests.forEach((t) => {
      const output = parseDate(t.input);
      debug(t.input, '->', output);
      assert.equal(output, t.correct, `Incorrectly parsed date:${t.input}->${output}`);
    });
  });
});
