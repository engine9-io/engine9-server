const util = require('node:util');
const debug = require('debug')('LocalPluginPersonWorker');
const { relativeDate } = require('../utilities');
const PersonWorker = require('./PersonWorker');

function Worker(worker) {
  PersonWorker.call(this, worker);
}

util.inherits(Worker, PersonWorker);

Worker.metadata = {};

/*
  A version of a PersonWorker devoted to locally stored data, often in a current database
*/

Worker.prototype.internalLoadFromTable = async function (options) {
  const { table: inTable, start, end } = options;

  const plugin = await this.getPlugin(options);
  const table = `${plugin.table_prefix || ''}${inTable}`;
  let desc;
  try {
    desc = await this.describe({ table });
    if (!desc.columns) throw new Error('No columns');
  } catch (e) {
    return { no_data: true, does_not_exist: table };
  }
  const dateColumn = [
    'frakture_last_modified',
    'ts',
    'remote_last_modified',
    'last_modified', 'frakture_date_created', 'date_created']
    .find((d) => desc.columns.find((c) => c.name === d));
  const conditions = [];
  if (start || end) {
    if (!dateColumn) throw new Error(`start/end specified, but no date column for table ${table}`);
    if (start) conditions.push(`${dateColumn}>=${this.escapeDate(relativeDate(start))}`);
    if (end) conditions.push(`${dateColumn}<${this.escapeDate(relativeDate(end))}`);
  }
  const ignore = ['id'];
  const includes = desc.columns.map((d) => {
    if (ignore.indexOf(d.name) < 0) return d.name;
    return null;
  }).filter(Boolean);

  return this.internalLoadPeopleFromDatabase({
    sql: `select ${includes.map((d) => this.escapeColumn(d)).join(',')},'${plugin.id}' as plugin_id from ${table} ${conditions.length > 0 ? `where ${conditions.join(' AND ')}` : ''}`,
    pluginId: plugin.id,
    remoteInputId: `${(options.remotePluginId || plugin.id)}.${table}`,
  });
};
const internalMeta = {
  options: {
    pluginId: {},
    remotePluginId: {},
    start: {},
    end: {},
  },
};

Worker.prototype.ensureSchema = async function () {
  return this.deployStandard();
};
Worker.prototype.ensureSchema.metadata = internalMeta;

Worker.prototype.importPeople = async function (options) {
  return this.internalLoadFromTable({ table: 'person', ...options });
};
Worker.prototype.importPeople.metadata = internalMeta;

Worker.prototype.importEmails = async function (options) {
  return this.internalLoadFromTable({ table: 'person_email', ...options });
};
Worker.prototype.importEmails.metadata = internalMeta;

Worker.prototype.importPhones = async function (options) {
  return this.internalLoadFromTable({ table: 'person_phone', ...options });
};
Worker.prototype.importPhones.metadata = internalMeta;

Worker.prototype.importAddresses = async function (options) {
  return this.internalLoadFromTable({ table: 'person_address', ...options });
};
Worker.prototype.importAddresses.metadata = internalMeta;

Worker.prototype.importTransactions = async function (options) {
  const { start, end } = options;

  const dbPlugin = await this.getPlugin({ path: 'workerbots.DBBot' });
  const globalPrefix = dbPlugin?.table_prefix || '';

  const plugin = await this.getPlugin(options);
  const table = `${plugin.table_prefix || ''}transaction`;
  debug('Describing table ', table);
  let desc;
  try {
    desc = await this.describe({ table });
    if (!desc.columns) throw new Error('No columns');
  } catch (e) {
    debug(`No such table:${table}`);
    return { no_data: true, does_not_exist: table };
  }
  const dateColumn = 'ts';

  const conditions = [];
  if (start || end) {
    if (!dateColumn) throw new Error(`start/end specified, but no date column for table ${table}`);
    if (start) conditions.push(`t.${dateColumn}>=${this.escapeDate(relativeDate(start))}`);
    if (end) conditions.push(`t.${dateColumn}<${this.escapeDate(relativeDate(end))}`);
  }
  const ignore = ['id', 'person_id'];
  /*
    TRANSACTION,
    TRANSACTION_INITIAL,
    TRANSACTION_SUBSEQUENT,
    TRANSACTION_REFUND,
  */

  const includes = ['m.person_id_int as person_id'].concat(desc.columns.map((d) => {
    if (ignore.indexOf(d.name) < 0) return `t.${this.escapeColumn(d.name)}`;
    return null;
  }).filter(Boolean));

  if (desc.columns.find((d) => d.name === 'entry_type')
    || desc.columns.find((d) => d.name === 'entry_type_id')
  ) {
    // We're okay -- if there's invalid data in those field we should know it
  } else if (desc.columns.find((d) => d.name === 'recurs')) {
    includes.push(`case when length(t.recurs)>0 
     then 'TRANSACTION_RECURRING'
     else 'TRANSACTION_ONE_TIME' end as entry_type`);
  } else {
    includes.push('\'TRANSACTION\' as entry_type');
  }

  const sql = `select ${includes.join(',')},
      '${plugin.id}' as plugin_id from ${table} t
      left join ${globalPrefix}transaction_metadata m
      on (t.remote_transaction_id=m.remote_transaction_id and
        m.transaction_bot_id='${plugin.remote_plugin_id}')
       ${conditions.length > 0 ? `where ${conditions.join(' AND ')}` : ''}`;
  debug('importTransactions SQL:', sql);

  return this.internalLoadPeopleFromDatabase({
    sql,
    pluginId: plugin.id,
    remoteInputId: `${(options.remotePluginId || plugin.id)}.${table}`,
    extraPostIdentityTransforms: [
      { path: 'engine9-interfaces/transaction/transforms/inbound/upsert_tables.js', options: {} },
    ],
  });
};
Worker.prototype.importTransactions.metadata = internalMeta;

module.exports = Worker;
