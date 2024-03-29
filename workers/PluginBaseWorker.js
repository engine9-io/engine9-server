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

Worker.prototype.compileTransform = async function ({ transform }) {
  if (typeof transform === 'function') return transform;
  // eslint-disable-next-line import/no-dynamic-require,global-require
  return require(transform);
};

Worker.prototype.compilePipeline = async function ({ pipeline: _pipeline }) {
  let pipeline = null;
  if (typeof _pipeline === 'string') {
    pipeline = JSON5.parse(await fsp.readFile(_pipeline));
  } else {
    pipeline = _pipeline;
  }

  const transforms = Promise.all(pipeline.transforms.map(({ transform }) => this.compileTransform({ transform })));
  return { transforms };
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
