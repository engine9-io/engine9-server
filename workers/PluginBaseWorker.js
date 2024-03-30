const util = require('util');
const fs = require('node:fs');

const fsp = fs.promises;
const JSON5 = require('json5');// Useful for parsing extended JSON

const BaseWorker = require('./BaseWorker');
const SQLWorker = require('./SQLWorker');
const FileWorker = require('./FileWorker');

function Worker(worker) {
  BaseWorker.call(this, worker);
}

util.inherits(Worker, BaseWorker);

/*
  Core method that takes an plugin configuration,
  creates all the environment variables, including SQL, etc
*/

const validPaths = /^[a-zA-Z-_]+$/; // Don't allow dots or anything crazy in path names - simple simple
Worker.prototype.compilePlugin = async function ({ pluginPath }) {
  if (!pluginPath?.match(validPaths)) throw new Error(`Invalid plugin path: ${pluginPath}`);
  // eslint-disable-next-line import/no-dynamic-require,global-require
  const config = require(`./${pluginPath}/engine9_plugin.js`);

  let sqlConnection = null;

  const streams = {};
  Object.entries(config.streams || {}).forEach(([name, streamConfig]) => {
    const output = {};
    streams[name] = output;
    output.batch_size = streamConfig.batch_size || 100;

    output.env = {};
    Object.entries(streamConfig.env).forEach(([k, v]) => {
      if (typeof v === 'string') {
        const parts = v.split('.');
        if (parts[0] === 'SQL') {
          if (sqlConnection === null) {
            sqlConnection = new SQLWorker().connect();
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
Worker.prototype.compileTransform = async function ({ transform, path, options }) {
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
        tablesToUpsert: { type: 'sql.tablesToUpsert' },
      },
      transform: (opts) => this.sqlWorker.upsertTables(opts),
    };
  }

  // eslint-disable-next-line import/no-dynamic-require,global-require
  const f = require(path);
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

  const transforms = await Promise.all(pipeline.transforms
    .map(({ transform, path, options }) => this.compileTransform({ transform, path, options })));

  return { transforms };
};

Worker.prototype.executeCompiledPipeline = async function ({ pipeline, batch }) {
  const tablesToUpsert = {};

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
      const transformParams = { batch, options };
      const bindingNames = Object.keys(bindings);
      // eslint-disable-next-line no-await-in-loop
      await Promise.all(bindingNames.map(async (name) => {
        const binding = bindings[name];
        if (!binding.type) throw new Error(`type is required for binding ${name}`);
        if (binding.type === 'sql.query') {
          if (!this.sqlWorker) this.sqlWorker = new SQLWorker(this);
          if (!binding.columns) throw new Error(`columns are required for binding ${name}`);
          if (binding.columns.length !== 1) throw new Error(`Currently only one column is allowed for sql.query bindings, found ${binding.columns.length} for ${name}`);
          const values = new Set();
          batch.forEach((b) => {
            const v = b[binding.columns[0]];
            if (v) values.add(v);
          });
          if (values.length === 0) {
            transformParams[name] = [];
            return;
          }
          const sql = `/* ${cleanPath} */ select * from ${this.sqlWorker.escapeTable(binding.table)}`
          + ` where ${this.sqlWorker.escapeColumn(binding.columns[0])} in (${[...values].map(() => '?').join(',')})`;
          const { data } = await this.sqlWorker.query(sql, [...values]);
          transformParams[name] = data;
        } else if (binding.type === 'sql.tablesToUpsert') {
          transformParams[name] = tablesToUpsert;
        }
      }));

      // eslint-disable-next-line no-await-in-loop
      await transform(transformParams);
    } catch (e) {
      this.destroy();
      throw e;
    }
  }
};

Worker.prototype.testPipeline = async function ({ stream, filename }) {
  const fileWorker = new FileWorker(this);
  const inStream = await fileWorker.getStream({ stream, filename });
  return inStream;
  /*  await pipeline(
    // do batching
    stream,
    emailExtension,
    through2.obj((o, enc, cb) => {
      debug('Through2:', o);
      cb(null, `${JSON.stringify(o)}\n`);
    }),
    fileStream,
  ); */
};

module.exports = Worker;
