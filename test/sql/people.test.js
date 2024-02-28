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
    { email: 'x@y.com' },
    { email: 'x@y.com' },
    { email: 'y@z.com' },
  ];
  for (let i = 0; i < 300; i += 1) {
    batch.push({ email: `${i % 50}@y.com` });
  }
  batch.forEach((d) => {
    d.identifiers = [{ type: 'email', value: d.email }];
  });
  const out = await worker1.appendPersonIds({ batch });

  knex.destroy();
  assert.ok(out?.[0]?.person_id > 0, 'No valid person_id was assigned');
});

test('Good tables are returned', async () => {
  const sqlWorker = new SQLWorker({ accountId: 'engine9' });
  const knex = await sqlWorker.connect();
  const worker1 = new PersonWorker({ accountId: 'engine9' });
  worker1.knex = knex;

  const batch = [
    { email: 'x@y.com' },
    { email: 'x@y.com' },
    { email: 'y@z.com' },
  ];

  batch.forEach((d) => {
    d.identifiers = [{ type: 'email', value: d.email }];
  });
  const out = await worker1.ingestPeople({ batch, doNotInsert: true });

  knex.destroy();
  console.log('Table output:', out);
  assert.ok(out?.person_email?.length > 0, 'No person_email records');
});
