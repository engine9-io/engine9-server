const { performance, PerformanceObserver } = require('node:perf_hooks');
const util = require('node:util');
const { pipeline } = require('node:stream/promises');
const { createHash } = require('node:crypto');
const { Transform } = require('node:stream');

const { v7: uuidv7 } = require('uuid');

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
  Okay, assigning ids is the one thing that may happen in parallel, so we
  need to do some blocking.
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
  const knex = await this.connect();

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
  const existingIdLookup = {};
  existingIds.forEach((row) => {
    existingIdLookup[row.id_value] = true;
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

  const tempIdToPersonIdLookup = {};
  if (toInsert.length > 0) {
    const response = await knex.table('person')
      .insert(toInsert);
    let currentId = response[0];

    tempIds.forEach((t) => {
      tempIdToPersonIdLookup[t] = currentId;
      currentId += 1;
    });
  }

  // Assign the person_ids to the batch,
  // and build up a person_identifier insert object

  const personIdentifersToInsert = {};
  batch.forEach((item) => {
    if (!item.person_id) {
      item.person_id = tempIdToPersonIdLookup[item.temp_id];
      if (!item.person_id) throw new Error(`Unusual error, could not find temp_id:${item.temp_id}`);
      delete item.temp_id;
    }
    (item.identifiers || []).forEach((id) => {
      if (existingIdLookup[id.value]) return;
      personIdentifersToInsert[id.value] = {
        person_id: item.person_id,
        // source_input_id is the connection this record first came from.  It
        // should be provided by the source stream, and will error if it's null.
        // If there truly is no input, provide a 0 uuid
        source_input_id: item.source_input_id,
        id_type: id.type,
        id_value: id.value,
      };
    });
  });

  const identifiersToInsert = Object.values(personIdentifersToInsert);
  if (identifiersToInsert.length > 0) {
    await knex.table('person_identifier')
      .insert(identifiersToInsert);
  }

  await knex.raw('unlock tables');
  /* Finished locking */
  return batch;
};
Worker.prototype.getSQLWorker = async function () {
  let { sqlWorker } = this;
  if (!sqlWorker) {
    sqlWorker = new SQLWorker(this);
    await sqlWorker.connect();
  }
  return sqlWorker;
};

Worker.prototype.appendPersonId = async function ({ batch, sourceInputId }) {
  const itemsWithNoIds = batch.filter((o) => !o.person_id);
  if (itemsWithNoIds.length === 0) return batch;
  const allIdentifiers = itemsWithNoIds.reduce((a, b) => a.concat(b.identifiers || []), []);
  const { knex } = await this.getSQLWorker();

  performance.mark('start-existing-id-sql');
  const existingIds = await knex.select(['id_value', 'person_id'])
    .from('person_identifier')
    .where('id_value', 'in', allIdentifiers.map((d) => d.value));
  performance.mark('end-existing-id-sql');
  const lookup = existingIds.reduce(
    (a, b) => {
      a[b.id_value] = b.person_id;
      return a;
    },
    {},
  );
  batch.forEach((item) => {
    if (item.person_id) return;
    const matchingValue = (item.identifiers || []).find((id) => lookup[id.value])?.value;
    if (matchingValue) item.person_id = lookup[matchingValue];
  });
  performance.mark('start-assign-ids-blocking');
  /*
  //so the problem with this is there may be more identifiers still in the input
  // that need to make it to the person_identifier table
  const itemsWithNoExistingIds = batch.filter((o) => !o.person_id);

  if (itemsWithNoExistingIds.length === 0) {
    performance.mark('end-assign-ids-blocking');
    return batch;
  }
  await this.assignIdsBlocking({ batch: itemsWithNoExistingIds, sourceInputId });
  */
  await this.assignIdsBlocking({ batch, sourceInputId });
  performance.mark('end-assign-ids-blocking');
  return batch;
};

Worker.prototype.getDefaultPipelineConfig = async function () {
  let customFields = [];

  const sqlWorker = await this.getSQLWorker();
  const { data: plugins } = await sqlWorker.query('select * from plugin where path=\'@engine9-interfaces/person_custom\'');
  customFields = await Promise.all(plugins.map(async (plugin) => {
    const table = `${plugin.table_prefix}field`;
    try {
      const desc = await sqlWorker.describe({ table });
      return {
        path: 'engine9-interfaces/person_custom/transforms/inbound/upsert_tables.js',
        options: { table, schema: plugin.schema, columns: desc.columns },
      };
    } catch (e) {
      // table may not exist
      return null;
    }
  }));

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
    ].concat(customFields.filter(Boolean))
    // { path: 'engine9-interfaces/person_address/transforms/inbound/upsert_tables.js' },
      // { path: 'engine9-interfaces/segment/transforms/inbound/upsert_tables.js' },
      .concat({ path: 'sql.upsertTables' }),
  };
};

Worker.prototype.loadPeople = async function (options) {
  const worker = this;
  const {
    stream, filename, packet, batchSize = 500,
  } = options;

  // inputId, pluginId, remoteInputId, inputType = 'unknown',
  const inputId = await this.getInputId(options);
  if (!inputId) throw new Error('Could not get a required input id from options');

  const fileWorker = new FileWorker(this);
  const inStream = await fileWorker.getStream({
    stream, filename, packet, type: 'person',
  });
  if (!worker.compiledPipeline) {
    const pipelineConfig = await this.getDefaultPipelineConfig();
    worker.compiledPipeline = await this.compilePipeline(pipelineConfig);
  }
  let records = 0;
  const start = new Date();
  const summary = { executionTime: {} };

  await pipeline(
    inStream.stream,
    this.getBatchTransform({ batchSize }).transform,
    new Transform({
      objectMode: true,
      async transform(batch, encoding, cb) {
        batch.forEach((b) => { b.source_input_id = b.source_input_id || inputId; });
        const batchSummary = await worker.executeCompiledPipeline(
          { pipeline: worker.compiledPipeline, batch },
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
  (this.compiledPipeline.newStreams || []).forEach((s) => s.push(null));
  // Await any file completions
  await Promise.all(this.compiledPipeline.promises || []);
  summary.files = this.compiledPipeline.files || [];
  summary.records = records;

  return summary;
};

Worker.prototype.loadPeople.metadata = {
  options: {
    stream: {},
    filename: {},
    batchSize: {},
    pluginId: {},
    inputId: {},
    remoteInputId: {},
    inputType: {},
    inputMetadata: {}, // metadata for new inputs
  },
};

Worker.prototype.loadPeopleFromDatabase = async function (options) {
  const { sql, pluginId } = options;
  if (!sql) throw new Error('sql is required');
  if (!pluginId) throw new Error('pluginId is required');

  const source = new SQLWorker(this);
  const stream = await source.stream({ sql });

  const remoteInputId = createHash('sha256')
    .update(sql)
    .digest('hex');

  return this.loadPeople({
    stream,
    remoteInputId,
    pluginId,
    inputType: 'sql',
    inputMetadata: JSON.stringify({
      sql: sql.slice(0, 1000),
    }),
  });
};
Worker.prototype.loadPeopleFromDatabase.metadata = {};

module.exports = Worker;
