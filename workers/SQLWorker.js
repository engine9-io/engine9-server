const util = require('util');
const Knex = require('knex');
const { SchemaInspector } = require('knex-schema-inspector');

const BaseWorker = require('./BaseWorker');

require('dotenv').config({ path: '.env' });

/* class Person extends Model {
  static get tableName() {
    return 'person';
  }
}
*/

function Worker(worker) {
  BaseWorker.call(this, worker);
}

util.inherits(Worker, BaseWorker);
Worker.metadata = {
  alias: 'sql',
};

Worker.prototype.connect = async function connect() {
  if (this.knex) return this.knex;
  const { accountId } = this;
  if (!this.accountId) throw new Error('accountId is required');

  let config = null;
  if (accountId === 'steamengine') {
    const s = process.env.STEAMENGINE_DATABASE_CONNECTION_STRING;
    if (!s) throw new Error('Could not find environment variable \'STEAMENGINE_DATABASE_CONNECTION_STRING\'');
    config = {
      client: 'mysql2',
      connection: process.env.STEAMENGINE_DATABASE_CONNECTION_STRING,
    };
  } else {
    throw new Error('Unsupported account:', accountId);
  }

  this.knex = Knex(config);
  return this.knex;
};
Worker.prototype.ok = async function f() {
  const knex = await this.connect();
  return knex.raw('select 1');
};
Worker.prototype.ok.metadata = {
  options: {},
};

Worker.prototype.showTables = async function f() {
  const knex = await this.connect();
  const inspector = SchemaInspector(knex);
  return inspector.tables();
};
Worker.prototype.showTables.metadata = {
  options: {},
};

module.exports = Worker;
