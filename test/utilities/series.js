const {
  describe, it,
} = require('node:test');
const debug = require('debug')('test/utilities/series');
const assert = require('node:assert');
const { setTimeout: asyncSetTimeout } = require('node:timers/promises');

describe('Test series execution', async () => {
  let finishCounter = 0;
  it('Should wait and be first', async () => {
    await asyncSetTimeout(1000);
    finishCounter += 1;
    debug('Finished 1');
    assert(finishCounter, 1, 'First test did not finish first');
  });
  it('Should wait and be second', async () => {
    await asyncSetTimeout(200);
    finishCounter += 1;
    debug('Finished 2');
    assert(finishCounter, 2, 'Second test did not finish second');
  });
  it('Should wait and be third', async () => {
    await asyncSetTimeout(100);
    finishCounter += 1;
    debug('Finished 3');
    assert(finishCounter, 3, 'Third test did not finish third');
  });
  it('Should wait and be fourth', async () => {
    await asyncSetTimeout(0);
    finishCounter += 1;
    debug('Finished 4');
    assert(finishCounter, 4, 'Fourth test did not finish fourth');
  });
});
