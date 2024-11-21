const util = require('node:util');

/* const { pipeline } = require('node:stream/promises');
const { uuidv7 } = require('uuidv7');
const { Transform } = require('node:stream');

const debug = require('debug')('PersonWorker');
const info = require('debug')('info:PersonWorker');

const FileWorker = require('./FileWorker');
*/
const PersonWorker = require('./PersonWorker');

function Worker(worker) {
  PersonWorker.call(this, worker);
}

util.inherits(Worker, PersonWorker);

/*
Worker.prototype.import = async ({ filename }) => {
  await personWorker.appendInputId({ pluginId, batch });
  await personWorker.appendEntryTypeId({ batch });
  await personWorker.appendSourceCodeId({ batch });
  await personWorker.appendPersonId({ batch });
  await personWorker.appendEntryId({ pluginId, batch });
};
*/

module.exports = Worker;
