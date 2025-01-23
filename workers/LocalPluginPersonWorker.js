const util = require('node:util');
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
    sql: `select ${includes.join(',')},'${plugin.id}' as plugin_id from ${table} ${conditions.length > 0 ? `where ${conditions.join(' AND ')}` : ''}`,
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

module.exports = Worker;
