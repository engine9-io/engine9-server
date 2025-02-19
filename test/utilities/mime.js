const {
  describe, it,
} = require('node:test');
const assert = require('node:assert');
// eslint-disable-next-line import/no-unresolved
const { mimeType: mime } = require('mime-type/with-db');

describe('Mime test', async () => {
  it('Should get correct json mime type', async () => {
    const json = mime.lookup('test.json');
    const correct = 'application/json';
    assert.equal(json, correct, `Analysis statistics not correct, ${json}!=${correct}`);
  });
});
