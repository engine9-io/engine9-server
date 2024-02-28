const util = require('node:util');
const { pipeline } = require('node:stream/promises');
const crypto = require('node:crypto');
// const { setTimeout: sleep } = require('node:timers/promises');

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
  const { knex } = this;

  if (!knex) {
    throw new Error('No knex instance created');
  }

  /*
  We're locking the tables so we can ensure that multiple threads against the database
  won't cause identical identifiers to be inserted.  Without it the same identifier could
  have 2 separate person_ids, which defeats the purpose
  */
  await knex.raw('lock tables person write,person_identifiers write');
  /* First check to see if any new IDS have slotted in here */
  const existingIds = await knex.select('*')
    .from('person_identifiers')
    .where('value', 'in', Object.keys(identifierMap));
  debug('Found ', existingIds, '.... sleeping');
  /* If we want to test the logic here, we can sleep after the query to wait
  for the conflicting thread to catch up to the problem zone
  */
  // await sleep(5000);
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

  const toInsert = tempIds.map(() => ({ id: null }));

  if (toInsert.length > 0) {
    const response = await knex.table('person')
      .insert(toInsert);
    let currentId = response[0];
    const tempIdToPersonIdLookup = {};
    tempIds.forEach((t) => {
      tempIdToPersonIdLookup[t] = currentId;
      currentId += 1;
    });

    // Assign the person_ids to the batch,
    // and build up a person_identifier insert object
    const personIdentifersToInsert = {};
    batch.forEach((item) => {
      if (!item.person_id) {
        item.person_id = tempIdToPersonIdLookup[item.temp_id];
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
      await knex.table('person_identifiers')
        .insert(identifiersToInsert);
    }
  }
  await knex.raw('unlock tables');
  /* Finished locking */
  return batch;
};

Worker.prototype.appendPersonIds = async function ({ batch }) {
  const itemsWithNoIds = batch.filter((o) => !o.person_id);
  if (itemsWithNoIds.length === 0) return batch;
  const allIdentifiers = itemsWithNoIds.reduce((a, b) => a.concat(b.identifiers || []), []);
  let { knex } = this;

  if (!knex) {
    const sqlWorker = new SQLWorker(this);
    this.knex = await sqlWorker.connect();
    knex = this.knex;
  }
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
  batch.forEach((item) => {
    if (item.person_id) return;
    const matchingValue = (item.identifiers || []).find((id) => lookup[id.value])?.value;
    if (matchingValue) item.person_id = lookup[matchingValue];
  });
  const itemsWithNoExistingIds = batch.filter((o) => !o.person_id);
  if (itemsWithNoExistingIds.length === 0) return batch;
  debug('Items with no ids:', JSON.stringify(itemsWithNoExistingIds));
  await this.assignIdsBlocking({ batch: itemsWithNoExistingIds });
  return batch;
};

Worker.prototype.ingestPeople = async function ({ batch: _batch, doNotInsert }) {
  const batch = _batch;
  const identifierPaths = [
    '../core_extensions/person_email/transforms/inbound/append_identifiers.js',
  ];
  // assign identifiers to the batch
  await Promise.all(
    identifierPaths.map(async (path) => {
      const transform = await this.compileTransform({ path });
      if (typeof transform !== 'function') {
        throw new Error(`Invalid transform path, not a function:${path}`);
      }
      return transform(batch);
    }),
  );
  // assign person ids
  await this.appendPersonIds({ batch });

  const tableTransforms = [
    '../core_extensions/person_email/transforms/inbound/extract_tables.js',
  ];
  const tables = {};
  // assign identifiers to the batch
  await Promise.all(
    tableTransforms.map(async (path) => {
      const transform = await this.compileTransform({ path });

      const batchTables = await transform(batch);
      console.log('Transform returned ', batchTables);
      Object.keys(batchTables).forEach((table) => {
        tables[table] = (tables[table] || []).concat(batchTables[table]);
      });
    }),
  );
  if (doNotInsert) return tables;
  return tables;
};
Worker.prototype.ingestPeople.metadata = {
  options: {
    stream: {},
    filename: {},
  },
};

module.exports = Worker;
