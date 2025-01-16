const debug = require('debug')('WorkerList');

const paths = {
  EchoWorker: './EchoWorker',
};

module.exports = function (n) {
  if (!paths[n]) throw new Error(`Could not find ${n}`);
  try {
  // eslint-disable-next-line import/no-dynamic-require, global-require
    return require(paths[n]);
  } catch (e) {
    debug(e);
    debug(`Could not require ${paths[n]} from directory ${process.cwd()}. Try using an absolute path, or npm install the correct library`);
    return null;
  }
};
module.exports.paths = paths;

function getMetadata() {
  const meta = {};
  Object.entries(paths).forEach(([key]) => {
    const workerDef = module.exports(key);

    const worker = {
      metadata: workerDef.metadata,
      methods: {},
    };
    // eslint-disable-next-line no-restricted-syntax
    for (const i in workerDef.prototype) {
      if (typeof workerDef.prototype[i] === 'function'
        && typeof workerDef.prototype[i].metadata === 'object'
      ) {
        worker.methods[i] = workerDef.prototype[i].metadata;
      }
    }
    // response.methods=utilities.js.sortKeys(response.methods);
    meta[`engine9.${key}`] = worker;
  });
  return meta;
}
if (require.main === module) {
  // eslint-disable-next-line no-console
  console.log(JSON.stringify(getMetadata(), null, 4));
}
