const util = require('util');
// const debug = require('debug')('SQLCleanWorker');
const { Transform } = require('node:stream');
const { parseDate } = require('../utilities');
const SQLWorker = require('./SQLWorker');

require('dotenv').config({ path: '.env' });

function Worker(worker) {
  SQLWorker.call(this, worker);
  this.accountId = worker.accountId;
  if (!this.accountId) throw new Error('No accountId provided to SegmentWorker constructor');
  if (worker.knex) {
    this.knex = worker.knex;
  } else {
    this.auth = {
      ...worker.auth,
    };
  }
}

util.inherits(Worker, SQLWorker);

Worker.prototype.cleanDate = async function cleanDate(options) {
  const { field, targetTable } = options;
  if (!field) throw new Error('date "field" is required');
  const targetField = options.targetField || `${field}_clean`;

  const dateTransform = new Transform({
    objectMode: true,
    transform(obj, enc, cb) {
      try {
        obj[targetField] = parseDate(obj[field]);
      } catch (e) {
        return cb(e);
      }
      return cb(null, obj);
    },
  });

  const raw = await this.stream(options);
  const stream = raw.pipe(dateTransform).on('error', (e) => {
    throw new Error(e);
  });
  return this.insertFromStream({ ...options, table: targetTable, stream });
};

Worker.prototype.cleanDate.metadata = {
  options: {
    sql: { description: 'Source SQL' },
    field: { description: 'Source field in the SQL' },
    targetField: { description: 'Target fieldname (default <field>_clean)' },
    targetTable: { description: 'Target table to insert to' },
  },
};

module.exports = Worker;
