const util = require('node:util');
const fs = require('node:fs');

const fsp = fs.promises;
const JSON5 = require('json5');// Useful for parsing extended JSON
const debug = require('debug')('PluginBaseWorker');
const {
  getUUIDv7, getInputUUID, TIMELINE_ENTRY_TYPES, uuidRegex, getTimelineOutputStream,
} = require('@engine9/packet-tools');
const { v5: uuidv5 } = require('uuid');

const { LRUCache } = require('lru-cache');
const { Mutex } = require('async-mutex');

const SchemaWorker = require('./SchemaWorker');

function Worker(worker) {
  SchemaWorker.call(this, worker);
  this.debugCounter = 0;
}

util.inherits(Worker, SchemaWorker);

/*
  Core method that takes an extension configuration,
  creates all the environment variables, including SQL, etc
*/

const validPaths = /^[a-zA-Z-_]+$/; // Don't allow dots or anything crazy in path names - simple simple
Worker.prototype.compilePlugin = async function ({ extensionPath }) {
  if (!extensionPath?.match(validPaths)) throw new Error(`Invalid extension path: ${extensionPath}`);
  // eslint-disable-next-line import/no-dynamic-require,global-require
  const config = require(`./${extensionPath}/engine9_extension.js`);

  let sqlConnection = null;

  const streams = {};
  Object.entries(config.streams || {}).forEach(([name, streamConfig]) => {
    const output = {};
    streams[name] = output;
    output.batchSize = streamConfig.batchSize || 100;

    output.env = {};
    Object.entries(streamConfig.env).forEach(([k, v]) => {
      if (typeof v === 'string') {
        const parts = v.split('.');
        if (parts[0] === 'SQL') {
          if (sqlConnection === null) {
            sqlConnection = new SchemaWorker(this).connect();
          }
          if (parts[1] === 'tables') {
            // TODO -- check allowed tables here
            output.env[k] = sqlConnection.from(parts[2]);
          } else {
            throw new Error(`Invalid SQL environment variable:${parts[1]}`);
          }
        } else {
          throw new Error(`Invalid environment value:${parts[0]}`);
        }
      } else if (typeof v === 'number') {
        output.env[k] = v;
      } else {
        throw new Error(`Invalid environment value:${v}`);
      }
    });
    return { streams };
  });
};

/*
  The input id is either stored in the database, or generated and
stored
*/
const inputMutex = new Mutex();
Worker.prototype.getInputId = async function (opts) {
  const {
    inputId, pluginId, remoteInputId, remoteInputName, inputType = 'unknown', inputMetadata = null,
  } = opts;
  if (inputId) return inputId;
  if (!pluginId || !remoteInputId) throw new Error('pluginId and remoteInputId are both required to create one');

  return inputMutex.runExclusive(async () => {
    try {
      try {
        await this.knex.raw('lock tables input write,plugin write');
      } catch (e) {
      // we may not have permissions here, but try anyway
      }
      const { data } = await this.query({ sql: 'select * from input where plugin_id=? and remote_input_id=?', values: [pluginId, remoteInputId] });
      if (data.length > 0) return data[0].id;
      const { data: plugin } = await this.query({ sql: 'select * from plugin where id=?', values: [pluginId] });
      if (plugin.length === 0) throw new Error(`No such plugin:${pluginId}`);
      const id = getInputUUID(pluginId, remoteInputId);
      await this.insertFromStream({
        table: 'input',
        upsert: true,
        stream: [{
          id,
          plugin_id: pluginId,
          remote_input_id: remoteInputId,
          remote_input_name: remoteInputName,
          input_type: inputType,
          metadata: inputMetadata || null,
        },
        ],
      });

      return id;
    } catch (e) {
      debug(e);
      throw e;
    } finally {
      await this.knex.raw('unlock tables');
    }
  });
};

/* Compiles the transform exclusively, bindings are handled elsewhere */
Worker.prototype.compileTransform = async function ({ transform, path, options = {} }) {
  if (typeof transform === 'function') {
    return {
      path: 'custom_transform',
      bindings: {},
      options,
      transform,
    };
  }
  if (transform) throw new Error('transform should be a function');
  if (path === 'person.appendPersonId') {
    return {
      path,
      bindings: {},
      options,
      transform: (opts) => this.appendPersonId(opts),
    };
  } if (path === 'sql.upsertTables') {
    return {
      path,
      bindings: {
        tablesToUpsert: { type: 'sql.tables.upsert' },
      },
      options,
      transform: (opts) => this.upsertTables(opts),
    };
  }
  let p = path;
  if (path.indexOf('engine9-interfaces/') === 0) p = `../../${path}`;
  // debug(`Requiring ${p} from directory ${process.cwd()}`);

  // eslint-disable-next-line import/no-dynamic-require,global-require
  const f = require(p);
  if (typeof f === 'function') return { path, bindings: {}, transform: f };
  return {
    path,
    bindings: f.bindings || {},
    options,
    transform: f.transform,
  };
};

Worker.prototype.compilePipeline = async function (_pipeline) {
  if (!_pipeline) throw new Error('pipeline is a required attribute');
  let pipeline = null;
  if (typeof _pipeline === 'string') {
    pipeline = JSON5.parse(await fsp.readFile(_pipeline));
  } else {
    pipeline = _pipeline;
  }
  pipeline.transforms = pipeline.transforms || [];
  const transformPromises = [];
  pipeline.transforms.forEach(({ transform, path, options }) => {
    transformPromises.push(this.compileTransform({ transform, path, options }));
  });

  const transforms = await Promise.all(transformPromises);

  return { transforms };
};

Worker.prototype.executeCompiledPipeline = async function ({
  pipeline, batch, sourceInputId, pluginId,
}) {
  // pipeline level bindings
  pipeline.bindings = pipeline.bindings || {};
  // New streams that are started during a pipeline,
  // that must be completed afterwards with a push(null)
  pipeline.newStreams = pipeline.newStreams || [];
  // promises to wait after completion.  Good for finishing files, etc
  pipeline.promises = pipeline.promises || [];
  // output files
  pipeline.files = pipeline.files || [];

  const tablesToUpsert = {};
  const summary = { records: batch.length, executionTime: {} };

  // eslint-disable-next-line no-restricted-syntax
  for (const {
    transform, bindings, options, path,
  } of pipeline.transforms) {
    try {
      if (!path) {
        throw new Error(`no path found in ${JSON.stringify({
          transform, bindings, options, path,
        })}`);
      }
      const cleanPath = path.replace(/^[a-zA-Z/_-]*/, '_');
      const transformArguments = {
        batch, options, sourceInputId, pluginId,
      };
      const bindingNames = Object.keys(bindings);
      /*
      Bindings are the heart of getting data into and out of a transform.  Bindings allow for
      including and comparing existing data, allowing setups for upserting data,
      as well as outputs of timeline entries, etc.
      */
      // eslint-disable-next-line no-await-in-loop
      await Promise.all(bindingNames.map(async (name) => {
        const binding = bindings[name];
        if (!binding.type) throw new Error(`type is required for binding ${name}`);
        if (pipeline.bindings[name]) {
          transformArguments[name] = pipeline.bindings[name];
        } else if (binding.type === 'packet.output.timeline') {
          const {
            stream: timelineStream, promises,
            files,
          } = await getTimelineOutputStream({});
          debug(`Creating a new output timeline for binding ${name}`);
          pipeline.bindings[name] = timelineStream;
          pipeline.newStreams = pipeline.newStreams.concat(timelineStream);
          pipeline.promises = pipeline.promises.concat(promises || []);
          pipeline.files = pipeline.files.concat(files);
          transformArguments[name] = timelineStream;
        } else if (binding.type === 'sql.query') {
          if (!binding.lookup) throw new Error(`lookup as an array is required for binding ${name}`);
          if (binding.lookup.length !== 1) throw new Error(`Currently only one lookup column is allowed for sql.query bindings, found ${binding.lookup.length} for ${name}`);
          const values = new Set();
          batch.forEach((b) => {
            const v = b[binding.lookup[0]];
            if (v) values.add(v);
          });
          if (values.size === 0) {
            transformArguments[name] = [];
            return;
          }
          const sql = `/* ${cleanPath} */ select * from ${this.escapeTable(binding.table)}`
          + ` where ${this.escapeColumn(binding.lookup[0])} in (${[...values].map(() => '?').join(',')})`;
          const { data } = await this.query({ sql, values: [...values] });
          transformArguments[name] = data;
        } else if (binding.type === 'sql.tables.upsert') {
          transformArguments[name] = tablesToUpsert;
        }
      }));
      const start = new Date().getTime();
      // eslint-disable-next-line no-await-in-loop
      await transform(transformArguments);
      const ms = new Date().getTime() - start;
      summary.executionTime[path] = (summary.executionTime[path] || 0) + ms;
    } catch (e) {
      this.destroy();
      throw e;
    }
  }
  return summary;
};

Worker.prototype.getActivePluginPaths = async function () {
  // this will be dynamic at some point
  const paths = [
    'engine9-interfaces/person',
    'engine9-interfaces/person_email',
    'engine9-interfaces/person_address',
    'engine9-interfaces/person_phone',
    'engine9-interfaces/segment',
    'engine9-interfaces/message',
    'engine9-interfaces/job',
    'engine9-interfaces/query',
    'engine9-interfaces/report',
  ];
  return { paths };
};
Worker.prototype.getActivePluginPaths.metadata = {
  options: {},
};

Worker.prototype.deployAllSchemas = async function () {
  const { paths } = await this.getActivePluginPaths();
  const availableSchemas = [];
  // We want to do these in series because they may have an ordering
  // to deploying the schemas

  // eslint-disable-next-line no-restricted-syntax
  for (const schema of paths) {
    try {
      // eslint-disable-next-line no-await-in-loop
      await this.resolveLocalSchemaPath(schema);
      availableSchemas.push(schema);
    } catch (e) {
      debug(e);
    }
  }
  // eslint-disable-next-line no-restricted-syntax
  for (const schema of availableSchemas) {
    debug(`deployAllSchemas: Deploying schema:${schema}`);
    // eslint-disable-next-line no-await-in-loop
    await this.deploy({ schema });
  }
  return availableSchemas;
};

Worker.prototype.deployAllSchemas.metadata = {
  options: {},
};

Worker.prototype.appendDatabaseIdWithCaching = async function ({
  batch,
  table,
  inputField,
  defaultInputFieldValue,
  outputField,
  additionalWhere = {},
  idColumn = 'id',

}) {
  const type = table;
  // cleanup
  batch.forEach((o) => {
    if (typeof o[inputField] === 'string') o[inputField] = o[inputField].trim();
  });
  let itemsWithNoIds = batch.filter((o) => {
    if (o[outputField]) return false;//
    // ensure the field exists, even if it doesn't have a value
    // 0 works for blank entries, such as blank source codes -- not sure this is right
    o[outputField] = 0;
    const inputVal = (o[inputField] || '').trim();
    if (!inputVal) { // if there's not an input field, check for defaults
      if (defaultInputFieldValue) {
        o[inputField] = defaultInputFieldValue;
        return true;
      }
      return false; // no input field, no default, nothing to lookup
    }
    return true; // there's an input value to lookup
  });
  if (itemsWithNoIds.length === 0) return batch;
  this.itemCaches = this.itemCaches || {};
  this.itemCaches[type] = this.itemCaches[type] || new LRUCache({ max: 10000 });
  itemsWithNoIds.forEach((o) => {
    o[outputField] = this.itemCaches[type].get(o[inputField]);
  });
  itemsWithNoIds = itemsWithNoIds.filter((o) => !o[outputField]);
  if (itemsWithNoIds.length === 0) return batch;

  const valuesToLookup = itemsWithNoIds
    .reduce((a, o) => {
      // debug(`Need to lookup:'${o[inputField]}'`);
      a.add(o[inputField]);
      return a;
    }, new Set());
  const knex = await this.connect();
  // debug('valuesToLookup', Array.from(valuesToLookup));

  const existingIds = await knex.select([`${idColumn} as id`, `${inputField} as lookup`])
    .from(table)
    .where(inputField, 'in', Array.from(valuesToLookup))
    .andWhere(additionalWhere);

  // Populate the cache
  existingIds.forEach((r) => {
    // debug(`Adding to cache:${type} '${r.lookup}' '${r.id}'`);
    this.itemCaches[type].set(r.lookup.trim(), r.id);
  });

  // Filter out ones in the database already
  itemsWithNoIds = itemsWithNoIds.filter((o) => {
    const id = this.itemCaches[type].get(o[inputField]);
    o[outputField] = id;
    if (!o[outputField]) return true;
    return false;
  });

  if (!table) throw new Error('Table required');
  this.descriptionCache = this.descriptionCache || {};
  if (!this.descriptionCache[table]) {
    this.descriptionCache[table] = await this.describe({ table });
  }
  const desc = this.descriptionCache[table];
  const idType = desc.columns.find((d) => d.name === idColumn)?.type;
  if (!idType) throw new Error(`No idType found for ${idColumn}`);
  if (idType === 'foreign_uuid') throw new Error(`Unsupported id type:${idType}`);

  const valuesToInsert = Object.values(itemsWithNoIds.reduce((a, b) => {
    a[b[inputField]] = {
      ...additionalWhere,
      [idColumn]: idType === 'id_uuid' ? getUUIDv7() : null,
      [inputField]: b[inputField],
    };
    // debug('Added to insert:', a[b[inputField]]);
    return a;
  }, {}));
  if (valuesToInsert.length > 0) { await knex.table(table).insert(valuesToInsert); }

  const newIds = await knex.select([`${idColumn} as id`, `${inputField} as lookup`])
    .from(table)
    .where(inputField, 'in', valuesToInsert.map((d) => d[inputField]))
    .andWhere(additionalWhere);

  // Populate the cache
  newIds.forEach((r) => this.itemCaches[type].set(r.lookup, r.id));

  itemsWithNoIds = itemsWithNoIds.filter((o) => {
    const id = this.itemCaches[type].get(o[inputField]);
    if (this.debugCounter < 5) {
      this.debugCounter += 1;
      // debug('Assigning Id', id, 'for', inputField, o[inputField]);
    }
    o[outputField] = id;
    if (!o[outputField]) return true;
    return false;
  });
  if (itemsWithNoIds.length > 0) {
    throw new Error(`Error assigning ${type} ids to some records, including ${JSON.stringify(itemsWithNoIds.slice(0, 3))}`);
  }

  return batch;
};

Worker.prototype.appendSourceCodeId = async function ({
  batch,
}) {
  return this.appendDatabaseIdWithCaching({
    batch,
    table: 'source_code_dictionary',
    inputField: 'source_code',
    outputField: 'source_code_id',
    idColumn: 'source_code_id',
  });
};

Worker.prototype.appendInputId = async function ({
  pluginId,
  remoteInputId,
  batch,
}) {
  if (!pluginId) throw new Error('pluginId is required to appendInputId');
  return this.appendDatabaseIdWithCaching({
    batch,
    table: 'input',
    inputField: 'remote_input_id',
    defaultInputFieldValue: remoteInputId,
    additionalWhere: { plugin_id: pluginId },
    outputField: 'input_id',
    idColumn: 'id',
  });
};

Worker.prototype.appendEntryTypeId = function ({
  batch,
  defaultEntryType,
}) {
  batch.forEach((o) => {
    if (o.entry_type_id !== undefined) return;
    const etype = o.entry_type || defaultEntryType;
    if (!etype) {
      throw new Error('No entry_type specified, specify a defaultEntryType');
    }
    const id = TIMELINE_ENTRY_TYPES[etype];
    if (id === undefined) throw new Error(`Invalid entry_type: ${etype}`);
    o.entry_type_id = id;
    if (!o.ts && etype === 'SOURCE_CODE_OVERRIDE') o.ts = '1970-01-01';// this specific type gets a default date
  });
};

/*
 Entry ids are either
 A) Provided by the incoming object, and assumed to be unique
 or
 B) Generated as a UUIDv5, using the input_id as the source UUID
    and swapping out the timestamp portion to assist with sorting
 provided by the input, or calculated from the
  ts+
*/
Worker.prototype.appendEntryId = async function ({
  inputId,
  batch,
}) {
  const req = ['ts', 'entry_type_id', 'person_id'];
  batch.forEach((b) => {
    if (b.id) return;
    /*
      Outside systems CAN specify a unique UUID as remote_entry_uuid,
      which will be used for updates, etc.
      If not, it will be generated using whatever info we have
    */
    if (b.remote_entry_uuid) {
      if (!uuidRegex.test(b.remote_entry_uuid)) throw new Error('Invalid remote_entry_uuid, it must be a UUID');
      b.id = b.remote_entry_uuid;
      return;
    }
    const missing = req.filter((d) => b[d] === undefined);// 0 could be an entry type value
    if (missing.length > 0) throw new Error(`Missing required fields to append an entry_id:${missing.join(',')}`);
    const idString = `${b.ts}-${b.person_id}-${b.entry_type_id}-${b.source_code_id}`;
    const inId = b.input_id || inputId;
    if (!inId) throw new Error('Error appending entry id, no input_id in the file, and no default inputId');
    // get a temp ID
    const uuid = uuidv5(idString, inId);
    // Change out the ts to match the v7 sorting.
    // But because outside specified remote_entry_uuid
    // may not match this standard, uuid sorting isn't guaranteed
    b.id = getUUIDv7(b.ts, uuid);
  });
};

Worker.prototype.sortEntries = async function ({
  batch,
}) {
  const fields = ['entry_id', 'ts', 'input_id', 'entry_type_id',
    'person_id', 'source_code_id'];
  return batch.map((o) => {
    const out = {};
    fields.forEach((f) => {
      out[f] = o[f];
      delete o[f];
    });
    return Object.assign(out, o);
  });
};

Worker.prototype.ensurePlugin = async function ({
  id,
  type,
  path,
  name,
  tablePrefix,
  schema, // either an object with schema data, to be deployed, or a path to a schema
  unique = false, // indicates it should be unique
}) {
  if (!path) throw new Error("A path is required, either 'local' for an inline plugin, or a path to the root of the plugin");

  // Some checks for local plugins
  if (type === 'local') {
    if (typeof schema === 'string') throw new Error('For local paths, schema must be an object');
    if (!id) throw new Error('For local paths, you must specify an id');
  }

  let query = { sql: 'select * from plugin where path=?', values: [path] };
  if (id) query = { sql: 'select * from plugin where id=?', values: [id] };

  const { data: plugins } = await this.query(query);
  if (unique && plugins.length > 1) throw new Error('Error in plugin table, there are more than one plugins configured with @engine9-interfaces/plugin');
  let plugin = plugins[0] || {};
  if (plugins.length === 0) {
    plugin = {
      id: id || getUUIDv7(),
      path,
      name: name || path,
      table_prefix: tablePrefix,
      schema,
    };
    await this.knex.table('plugin').insert([plugin]);
  }
  if (plugin.schema) {
    await this.deploy({ schema: plugin.schema });
  }
  return plugin;
};

Worker.prototype.getSettings = async function ({ pluginId }) {
  const { data: settingsArr } = await this.query({ sql: 'select * from setting where plugin_id=?', values: [pluginId] });
  const settings = settingsArr.reduce((s, r) => {
    s[r.name] = r.value;
    return s;
  }, {});
  return settings;
};

Worker.prototype.setSetting = async function ({ pluginId, name, value }) {
  await this.insertFromStream({ table: 'setting', upsert: true, stream: [{ plugin_id: pluginId, name, value }] });
};

/* finds the next available table prefix */
Worker.prototype.getNextTablePrefixCounter = async function () {
  const plugin = await this.ensurePlugin({
    id: '00000000-0000-0000-0000-000000000001',
    path: '@engine9-interfaces/plugin',
    name: 'Core Plugin',
    unique: true,
  });
  const settings = await this.getSettings({ pluginId: plugin.id });

  let value = parseInt(settings?.table_prefix_counter || 2729, 10);// start with aaa
  value += 1;
  await this.setSetting({ pluginId: plugin.id, name: 'table_prefix_counter', value });
  return value.toString(16);
};

Worker.prototype.getNextTablePrefixCounter.metadata = {
  options: {},
};
Worker.prototype.getPlugin = async function (opts) {
  const { path, remotePluginId, pluginId } = opts;

  const conditions = [];
  const values = [];
  if (pluginId) {
    if (uuidRegex.test(pluginId)) {
      conditions.push('id=?');
      values.push(pluginId);
    } else {
      throw new Error(`Invalid pluginId, not a UUID:${pluginId}`);
    }
  }
  if (path) {
    conditions.push('path=?');
    values.push(path);
  }
  if (remotePluginId) {
    conditions.push('remote_plugin_id=?');
    values.push(remotePluginId);
  }
  if (conditions.length === 0) throw new Error(`getPlugin requires path, remotePluginId, or pluginId, not found in ${JSON.stringify(opts)}`);

  const { data: plugins } = await this.query({ sql: `select * from plugin where ${conditions.join(' AND ')}`, values });
  if (plugins.length === 0) throw new Error(`No available plugin with conditions ${conditions.join(' AND ')}`);
  if (plugins.length > 1) throw new Error(`Multiple plugins with condigions ${conditions.join(' AND ')}`);
  return plugins[0];
};
Worker.prototype.getPlugin.metadata = {
  options: {
    pluginId: {},
    remotePluginid: {},
    path: {},
  },
};

module.exports = Worker;
