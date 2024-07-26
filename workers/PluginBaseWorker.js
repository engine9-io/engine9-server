const util = require('node:util');
const fs = require('node:fs');

const fsp = fs.promises;
const JSON5 = require('json5');// Useful for parsing extended JSON
const debug = require('debug')('PluginBaseWorker');

const PacketTools = require('@engine9/packet-tools');
const SchemaWorker = require('./SchemaWorker');

function Worker(worker) {
  SchemaWorker.call(this, worker);
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

/* Compiles the transform exclusively, bindings are handled elsewhere */
Worker.prototype.compileTransform = async function ({ transform, path }) {
  if (typeof transform === 'function') {
    return {
      path: 'custom_transform',
      bindings: {},
      transform,
    };
  }
  if (transform) throw new Error('transform should be a function');
  if (path === 'person.appendPersonIds') {
    return {
      path,
      bindings: {},
      transform: (opts) => this.appendPersonIds(opts),
    };
  } if (path === 'sql.upsertTables') {
    return {
      path,
      bindings: {
        tablesToUpsert: { type: 'sql.tables.upsert' },
      },
      transform: (opts) => this.upsertTables(opts),
    };
  }
  let p = path;
  if (path.indexOf('engine9-interfaces/') === 0) p = `../../${path}`;
  debug(`Requiring ${p} from directory ${process.cwd()}`);

  // eslint-disable-next-line import/no-dynamic-require,global-require
  const f = require(p);
  if (typeof f === 'function') return { path, bindings: {}, transform: f };
  return {
    path,
    bindings: f.bindings || {},
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

Worker.prototype.executeCompiledPipeline = async function ({ pipeline, batch }) {
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
      const transformArguments = { batch, options };
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
          } = await PacketTools.getTimelineOutputStream({});
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
          if (values.length === 0) {
            transformArguments[name] = [];
            return;
          }
          const sql = `/* ${cleanPath} */ select * from ${this.escapeTable(binding.table)}`
          + ` where ${this.escapeColumn(binding.lookup[0])} in (${[...values].map(() => '?').join(',')})`;
          const { data } = await this.query(sql, [...values]);
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

module.exports = Worker;
