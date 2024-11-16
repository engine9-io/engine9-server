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

Worker.prototype.getDefaultPipelineConfig = async function () {
  return {
    transforms: [
      { path: 'engine9-interfaces/person_email/transforms/inbound/extract_identifiers.js', options: { dedupe_with_email: true } },
      /* { path: 'engine9-interfaces/person_phone/transforms/inbound/extract_identifiers.js',
          options: { dedupe_with_phone: true } }, */
      { path: 'person.appendPersonIds' },
      { path: 'engine9-interfaces/person_email/transforms/inbound/upsert_tables.js', options: {} },
      { path: 'sql.upsertTables' },
    ],
  };
};

module.exports = Worker;
