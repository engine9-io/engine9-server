const util = require('node:util');
const { pipeline } = require('node:stream/promises');
const crypto = require('node:crypto');

const through2 = require('through2');
// const fs = require('fs');
const debug = require('debug')('PersonWorker');
const SQLWorker = require('./SQLWorker');

const ExtensionBaseWorker = require('./ExtensionBaseWorker');

function Worker(worker) {
  ExtensionBaseWorker.call(this, worker);
}

util.inherits(Worker, ExtensionBaseWorker);

Worker.prototype.getPeopleStream = async function () {
  const sqlWorker = new SQLWorker(this);
  const emailExtension = this.compileExtension({ extension_path: 'core_extensions/person_email' });
  const stream = await sqlWorker.stream({ sql: 'select * from person' });

  const { filename, stream: fileStream } = this.getFileWriterStream();
  await pipeline(
    stream,
    emailExtension,
    // this.getJSONStringifyStream().stream,
    through2.obj((o, enc, cb) => {
      debug('Through2:', o);
      cb(null, `${JSON.stringify(o)}\n`);
    }),
    fileStream,
  );

  return { filename };
};

Worker.prototype.getPeopleStream.metadata = {};

/*
  Okay, assigning ids is the one thing that we need to parallelize.
  Presume two records are entering the database at the same time from
  multiple threads.

*/
Worker.prototype.assignIdsBlocking = async function ({ batch }) {
  const tempIdLookup = {};
  // Assign temp ids to everyone
  // This is because multiple entries in this batch could have the same ID
  batch.forEach((item) => {
    (item.identifiers || []).some((id) => {
      const tempId = tempIdLookup[id.value];
      if (tempId) {
        item.temp_id = tempId;
        return true;
      }
      return false;
    });
    if (!item.temp_id) {
      item.temp_id = crypto.randomUUID();
      (item.identifiers || []).forEach((id) => {
        tempIdLookup[id.value] = item.temp_id;
      });
    }
  });
  const identifierMap = batch.reduce(
    (a, b) => {
      (b.identifiers || [])
        .forEach((id) => {
          a[id.value] = (a[id.value] || []).concat(b);
        });
      return a;
    },
    {},
  );
  const sqlWorker = new SQLWorker(this);
  const knex = await sqlWorker.connect();

  /* lock starts here */
  knex.raw('lock table person_identifiers write');
  /* First check to see if any new IDS have slotted in here */
  const existingIds = await knex.select('*')
    .from('person_identifiers')
    .where('value', 'in', Object.keys(identifierMap));
  existingIds.forEach((row) => {
    (identifierMap[row.value] || []).forEach((item) => {
      item.person_id = row.person_id;
      delete item.temp_id;
    });
  });
  /* Now find the list of items that still need an ID */
  const tempIds = Object.keys(batch.filter((item) => item.temp_id)
    .reduce((a, b) => { a[b.temp_id] = b; return a; }, {}));
  // Insert the right number of records
  debug('Looking to insert ', tempIds);
  const toInsert = tempIds.map(() => ({ id: null }));
  debug('Looking to insert ', toInsert.length, 'records');
  if (toInsert.length > 0) {
    const response = await knex.table('person')
      .insert(toInsert);
    debug('Response from insert is:', { tempIds, response });
    let currentId = response[0];
    const tempIdToPersonIdLookup = {};
    tempIds.forEach((t) => {
      tempIdToPersonIdLookup[t] = currentId;
      currentId += 1;
      debug('Lookup is ', tempIds, tempIdToPersonIdLookup);
    });

    // Assign the person_ids to the batch,
    // and build up a person_identifier insert object
    const personIdentifersToInsert = {};
    batch.forEach((item) => {
      if (!item.person_id) {
        item.person_id = tempIdToPersonIdLookup[item.temp_id];
        debug('Assigned a person id identifier to', item);
        if (!item.person_id) throw new Error(`Unusual error, could not find temp_id:${item.temp_id}`);
        delete item.temp_id;
        (item.identifiers || []).forEach((id) => {
          personIdentifersToInsert[id.value] = {
            person_id: item.person_id,
            type: id.type,
            value: id.value,
          };
        });
      }
    });
    const identifiersToInsert = Object.values(personIdentifersToInsert);
    if (identifiersToInsert.length > 0) {
      debug('Inserting ', { identifiersToInsert });
      await knex.table('person_identifiers')
        .insert(identifiersToInsert);
    }
  }
  knex.raw('unlock table');
  /* Finished locking */
  return batch;
};

Worker.prototype.appendPersonId = async function ({ batch }) {
  const itemsWithNoIds = batch.filter((o) => !o.person_id);
  if (itemsWithNoIds.length === 0) return batch;
  const allIdentifiers = itemsWithNoIds.reduce((a, b) => a.concat(b.identifiers || []), []);
  const sqlWorker = new SQLWorker(this);
  const knex = await sqlWorker.connect();
  const existingIds = await knex.select('*')
    .from('person_identifiers')
    .where('value', 'in', allIdentifiers.map((d) => d.value));
  const lookup = existingIds.reduce(
    (a, b) => {
      a[b.value] = b.person_id;
      return a;
    },
    {},
  );
  debug('Existing item lookup=', lookup);
  batch.forEach((item) => {
    if (item.person_id) return;
    const matchingValue = (item.identifiers || []).find((id) => {
      debug('looking up value:', id.value);
      return lookup[id.value];
    })?.value;
    if (matchingValue) item.person_id = lookup[matchingValue];
  });
  const itemsWithNoExistingIds = batch.filter((o) => !o.person_id);
  if (itemsWithNoExistingIds.length === 0) return batch;
  debug('Items with no ids:', JSON.stringify(itemsWithNoExistingIds));
  await this.assignIdsBlocking({ batch: itemsWithNoExistingIds });
  return batch;
};

Worker.prototype.testAppendPersonId = async function () {
  const batch = [
    { email: 'x@y.com' },
    { email: 'x@y.com' },
    { email: 'y@z.com' },
  ];
  batch.forEach((d) => {
    d.identifiers = [{ type: 'email', value: d.email }];
  });
  const output = await this.appendPersonId({ batch });
  return output;
};
Worker.prototype.testAppendPersonId.metadata = {
  options: {},
};

Worker.prototype.loadPeople = async function ({ batch: _batch }) {
  const batch = _batch;
  const identifierPaths = [
    '../core_extensions/person_email/transforms/inbound/append_identifiers.js',
  ];
  const transforms = identifierPaths.map((path) => this.compileTransform({ path }));
  transforms.forEach(async (transform) => {
    await transform(batch);
  });

  /*
  1) get data
  2) Deduplicate, including inserting new people, thus getting everyone a person_id
  3) Go through and deduplicate the other items
  4) Insert and update
  */
};
Worker.prototype.loadPeople.metadata = {
  options: {
    stream: {},
    filename: {},
  },
};

module.exports = Worker;
