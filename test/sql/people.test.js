const test = require('node:test');
const assert = require('node:assert');
const PersonWorker = require('../../workers/PersonWorker');
const SQLWorker = require('../../workers/SQLWorker');

test('Ids are assigned by email', async () => {
  const sqlWorker = new SQLWorker({ accountId: 'engine9' });
  const knex = await sqlWorker.connect();
  const worker1 = new PersonWorker({ accountId: 'engine9' });
  worker1.knex = knex;

  const batch = [
    { email: 'emailassigntest@test.test' },
  ];
  const out = await worker1.appendPersonId({ batch });

  // This test passes because the Promise returned by the async
  // function is settled and not rejected.
  knex.destroy();
  assert.ok(out?.[0]?.person_id > 0, 'No valid person_id was assigned');
});
