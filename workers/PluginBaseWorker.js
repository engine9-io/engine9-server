const util = require('node:util');
const fs = require('node:fs');

const fsp = fs.promises;
const JSON5 = require('json5');// Useful for parsing extended JSON
const debug = require('debug')('PluginBaseWorker');

const PacketTools = require('engine9-packet-tools');
const BaseWorker = require('./BaseWorker');
const SQLWorker = require('./SQLWorker');

function Worker(worker) {
  BaseWorker.call(this, worker);
}

util.inherits(Worker, BaseWorker);

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
            sqlConnection = new SQLWorker(this).connect();
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
    if (!this.sqlWorker) this.sqlWorker = new SQLWorker(this);
    return {
      path,
      bindings: {
        tablesToUpsert: { type: 'sql.tables.upsert' },
      },
      transform: (opts) => this.sqlWorker.upsertTables(opts),
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
          pipeline.bindings[name] = timelineStream;
          pipeline.promises = pipeline.promises.concat(promises || []);
          pipeline.files = pipeline.files.concat(files);
          transformArguments[name] = timelineStream;
        } else if (binding.type === 'sql.query') {
          if (!this.sqlWorker) this.sqlWorker = new SQLWorker(this);
          if (!binding.columns) throw new Error(`columns are required for binding ${name}`);
          if (binding.columns.length !== 1) throw new Error(`Currently only one column is allowed for sql.query bindings, found ${binding.columns.length} for ${name}`);
          const values = new Set();
          batch.forEach((b) => {
            const v = b[binding.columns[0]];
            if (v) values.add(v);
          });
          if (values.length === 0) {
            transformArguments[name] = [];
            return;
          }
          const sql = `/* ${cleanPath} */ select * from ${this.sqlWorker.escapeTable(binding.table)}`
          + ` where ${this.sqlWorker.escapeColumn(binding.columns[0])} in (${[...values].map(() => '?').join(',')})`;
          const { data } = await this.sqlWorker.query(sql, [...values]);
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

module.exports = Worker;
