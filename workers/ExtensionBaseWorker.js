const util = require('util');

const BaseWorker = require('./BaseWorker');
const SQLWorker = require('./SQLWorker');

function Worker(worker) {
  BaseWorker.call(this, worker);
}

util.inherits(Worker, BaseWorker);

/*
  Core method that takes a configuration, creates all the environment variables, including SQL, etc
*/

const validPaths = /^[a-zA-Z-_]+$/; // Don't allow dots or anything crazy in path names - simple simple
Worker.prototype.compileExtension = async function compileStream({ extensionPath }) {
  if (!extensionPath?.match(validPaths)) throw new Error(`Invalid extension path: ${extensionPath}`);
  // eslint-disable-next-line import/no-dynamic-require,global-require
  const config = require(`./${extensionPath}/steamengine_extension.js`);

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

module.exports = Worker;
