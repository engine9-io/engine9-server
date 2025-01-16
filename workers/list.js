const debug = require('debug')('WorkerList');

const paths = {
  Engine9Workers: './Engine9Workers',
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
