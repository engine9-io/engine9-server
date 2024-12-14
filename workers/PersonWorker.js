const { performance, PerformanceObserver } = require('node:perf_hooks');

const util = require('node:util');
const { pipeline } = require('node:stream/promises');
const { v7: uuidv7 } = require('uuid');
const { Transform } = require('node:stream');

// const debug = require('debug')('PersonWorker');
// const info = require('debug')('info:PersonWorker');
const debugPerformance = require('debug')('Performance');
const SQLWorker = require('./SQLWorker');
const FileWorker = require('./FileWorker');
const PluginBaseWorker = require('./PluginBaseWorker');

const perfObserver = new PerformanceObserver((items) => {
  items.getEntries().forEach((entry) => {
    debugPerformance('%o', entry);
  });
});

perfObserver.observe({ entryTypes: ['measure'], buffer: true });

function Worker(worker) {
  PluginBaseWorker.call(this, worker);
}

util.inherits(Worker, PluginBaseWorker);

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
      item.temp_id = uuidv7();
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
  await knex.raw('lock tables person write,person_identifier write');
  /* First check to see if any new IDS have slotted in here */
  const existingIds = await knex.select(['id_value', 'person_id'])
    .from('person_identifier')
    .where('id_value', 'in', Object.keys(identifierMap));
  // debug('Found ', existingIds, '.... sleeping');
  /* If we want to test the logic here, we can sleep after the query to wait
  for the conflicting thread to catch up to the problem zone
  */
  // await sleep(5000);
  existingIds.forEach((row) => {
    (identifierMap[row.id_value] || []).forEach((item) => {
      item.person_id = row.person_id;
      delete item.temp_id;
    });
  });
  /* Now find the deduplicated list of items that still need an ID */
  const lookupByTempId = batch.filter((item) => item.temp_id)
    .reduce((a, b) => { a[b.temp_id] = b; return a; }, {});
  const tempIds = Object.keys(lookupByTempId);

  // Insert the right number of records

  // eslint-disable-next-line no-unused-vars
  const toInsert = tempIds.map((id) => ({
    id: null,
  //  given_name: lookupByTempId[id]?.given_name || '',
    // family_name: lookupByTempId[id]?.family_name || ''
  }));

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
            // source_input_id is the connection this record first came from.  It
            // should be provided by the source stream, or null
            source_input_id: item.input_id || null,
            id_type: id.type,
            id_value: id.value,
          };
        });
      }
    });
    const identifiersToInsert = Object.values(personIdentifersToInsert);
    if (identifiersToInsert.length > 0) {
      await knex.table('person_identifier')
        .insert(identifiersToInsert);
    }
  }
  await knex.raw('unlock tables');
  /* Finished locking */
  return batch;
};

Worker.prototype.appendPersonId = async function ({ batch }) {
  const itemsWithNoIds = batch.filter((o) => !o.person_id);
  if (itemsWithNoIds.length === 0) return batch;
  const allIdentifiers = itemsWithNoIds.reduce((a, b) => a.concat(b.identifiers || []), []);
  let { knex } = this;

  if (!knex) {
    const sqlWorker = new SQLWorker(this);
    this.knex = await sqlWorker.connect();
    knex = this.knex;
  }
  performance.mark('start-existing-id-sql');
  const existingIds = await knex.select(['id_value', 'person_id'])
    .from('person_identifier')
    .where('id_value', 'in', allIdentifiers.map((d) => d.value));
  performance.mark('end-existing-id-sql');
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
  performance.mark('start-assign-ids-blocking');
  if (itemsWithNoExistingIds.length === 0) {
    performance.mark('end-assign-ids-blocking');
    return batch;
  }
  // debug('Items with no ids:', JSON.stringify(itemsWithNoExistingIds));

  await this.assignIdsBlocking({ batch: itemsWithNoExistingIds });
  performance.mark('end-assign-ids-blocking');
  return batch;
};

Worker.prototype.getDefaultPipelineConfig = async function () {
  return {
    transforms: [
      { path: 'engine9-interfaces/person_remote/transforms/inbound/extract_identifiers.js', options: { } },
      { path: 'engine9-interfaces/person_email/transforms/inbound/extract_identifiers.js', options: { dedupe_with_email: true } },
      {
        path: 'engine9-interfaces/person_phone/transforms/inbound/extract_identifiers.js',
        options: { dedupe_with_phone: true },
      },
      { path: 'person.appendPersonId' },
      { path: 'engine9-interfaces/person_email/transforms/inbound/upsert_tables.js', options: {} },
      { path: 'engine9-interfaces/person_phone/transforms/inbound/upsert_tables.js', options: {} },
      // { path: 'engine9-interfaces/person_address/transforms/inbound/upsert_tables.js' },
      // { path: 'engine9-interfaces/segment/transforms/inbound/upsert_tables.js' },
      { path: 'sql.upsertTables' },
    ],
  };
};

Worker.prototype.upsertPersonBatch = async function ({ batch }) {
  if (!batch) throw new Error('upsert requires a batch');
  if (!this.compiledPipeline) {
    const pipelineConfig = await this.getDefaultPipelineConfig();
    this.compiledPipeline = await this.compilePipeline(pipelineConfig);
  }
  // info(`Executing pipeline with batch of size ${batch.length}`);
  const summary = await this.executeCompiledPipeline({ pipeline: this.compiledPipeline, batch });

  return summary;
};

Worker.prototype.upsert = async function ({
  stream, filename, packet, batchSize = 500,
}) {
  const worker = this;
  const fileWorker = new FileWorker(this);
  const inStream = await fileWorker.getStream({
    stream, filename, packet, type: 'person',
  });
  const pipelineConfig = await this.getDefaultPipelineConfig();
  const compiledPipeline = await this.compilePipeline(pipelineConfig);
  let records = 0;
  const start = new Date();
  const summary = { executionTime: {} };

  await pipeline(
    inStream.stream,
    this.getBatchTransform({ batchSize }).transform,
    new Transform({
      objectMode: true,
      async transform(batch, encoding, cb) {
        const batchSummary = await worker.executeCompiledPipeline(
          { pipeline: compiledPipeline, batch },
        );
        Object.entries(batchSummary.executionTime).forEach(([path, val]) => {
          summary.executionTime[path] = (summary.executionTime[path] || 0) + val;
        });
        records += batch.length;
        worker.logSome('PersonWorker upsert processed ', records, start, JSON.stringify({ filename, packet, ...summary }));
        cb();
      },
    }),
  );
  performance.measure('existing-ids', 'start-existing-id-sql', 'end-existing-id-sql');
  performance.measure('assign-ids', 'start-assign-ids-blocking', 'end-assign-ids-blocking');

  // There are some pipeline-wide streams and promises
  // like new timeline streams, or outputs to a packet or timeline file
  // terminate the inputs for these streams
  (compiledPipeline.newStreams || []).forEach((s) => s.push(null));
  // Await any file completions
  await Promise.all(compiledPipeline.promises || []);
  summary.files = compiledPipeline.files || [];
  summary.records = records;

  return summary;
};

Worker.prototype.upsert.metadata = {
  options: {
    stream: {},
    filename: {},
    batchSize: {},
  },
};

module.exports = Worker;
