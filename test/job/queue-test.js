require('dotenv').config({ path: '../../.env' });
const {
  describe, it,
} = require('node:test');
const assert = require('node:assert');

process.env.DEBUG = '*';
const debug = require('debug')('job-test');

let BASE_URL = process.env.ENGINE9_JOB_API_URL || 'http://localhost';
while (BASE_URL.slice(-1) === '/') {
  BASE_URL = BASE_URL.slice(0, -1);
}
if (!BASE_URL) throw new Error('Invalid ENGINE9_JOB_API_URL');

describe('Get a job, then update it', async () => {
  // const accountId = 'engine9';
  it('Should be able to retrieve a sample job from the job api', async () => {
    const url = `${BASE_URL}/jobs?test=true`;
    debug('Fetching URI', url);
    const r = await fetch(url);
    if (r.status >= 300) {
      debug('GET', r.status, url);
      throw new Error('Could not find a sample job');
    }
    const { data: jobs } = await r.json();
    assert.ok(Array.isArray(jobs), 'Not an array');
  });

  it('Should be able update a sample job from the api', async () => {
    const url = `${BASE_URL}/jobs/test`;
    const r = await fetch(url, {
      method: 'PATCH',
    });
    if (r.status >= 300) {
      debug('GET', r.status, url);
      throw new Error(`Could not patch a job with url ${url},status=${r.status}`);
    }
    const output = await r.json();
    assert.ok(output?.meta?.ok, `Invalid response:${JSON.stringify(output)}`);
  });
});
