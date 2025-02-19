/*
  Schema worker extends the SQLWorker to specifically work with DDL and schema definitions,
  as opposed to DML work
*/
const util = require('util');
const debug = require('debug')('SchemaWorker');
const fs = require('node:fs');

const fsp = fs.promises;
const path = require('node:path');
const JSON5 = require('json5');// Useful for parsing extended JSON
const SQLWorker = require('./SQLWorker');
const { ErrorWithMetadata: Error } = require('./Errors');
const mysqlDialect = require('./sql/dialects/MySQL');

function Worker(worker) {
  SQLWorker.call(this, worker);

  this.dialect = mysqlDialect;
}

util.inherits(Worker, SQLWorker);
Worker.metadata = {
  alias: 'schema',
};

/*
  Gets the path for a local schema, or throws an error
  if it does not exist
*/
Worker.prototype.resolveLocalSchemaPath = async function (schema) {
  if (!schema) throw new Error(`Could not resolve local schema path for schema:${schema}`);
  const localPath = path.resolve(`${__dirname}/../../${schema}${schema.slice(-1) === '/' ? '' : '/'}schema.js`);

  await fsp.access(localPath, fs.constants.R_OK);
  return localPath;
};

/*
  Retrieves, validates, and expands a schema to the global standard.
  Will throw an error if there's a problem with it
*/
Worker.prototype.standardize = async function ({ schema: _schema }) {
  if (!_schema) throw new Error('schema is required');
  let schema = null;
  if (typeof _schema === 'object') {
    schema = _schema;
  } else if (typeof _schema === 'string' && _schema.indexOf('engine9-interfaces/') === 0) {
    // This is a local version, not a github version
    const p = await this.resolveLocalSchemaPath(_schema);
    debug('Loading local schema:', p);
    // eslint-disable-next-line import/no-dynamic-require,global-require
    schema = require(p);
  } else {
    let content = null;
    if (_schema.indexOf('@engine9-interfaces/') === 0) {
      const name = _schema.slice('@engine9-interfaces/'.length);
      if (!name.match(/^[a-z0-9_-]+$/)) throw new Error('Invalid schema name');
      const uri = `https://raw.githubusercontent.com/engine9-io/engine9-interfaces/main/${name}/schema.js`;
      debug('Fetching URI', uri);
      const r = await fetch(uri);
      if (r.status >= 300) {
        debug('GET', r.status, uri);
        throw new Error(`Could not find schema ${_schema}`);
      }
      content = await r.text();
    } else {
      debug('schema does not start with @engine9-interfaces/, trying local file');
      content = await fs.promises.readFile(await this.resolveLocalSchemaPath(_schema));
    }

    if (!content) throw new Error(`No content found for ${_schema}`);
    content = content.toString().trim();
    if (content.indexOf('module.exports = ') === 0) { content = content.slice(17); }
    if (content.slice(-1) === ';') { content = content.slice(0, -1); }
    try {
      schema = JSON5.parse(content);
    } catch (error) {
      debug(content);
      throw new Error(`Error attempting to parse schema file at ${_schema}, ${error.message}`);
    }
  }
  // Create a deep copy, but clear out any functions, etc, those will need to be
  // handled elsewhere if they're included, this method is for database work
  try {
    const standardSchema = JSON.parse(JSON.stringify(schema));
    const invalidTables = [];
    standardSchema.tables = (standardSchema.tables || []).map((table) => {
      const invalidColumns = [];
      const columns = table.columns || [];
      table.columns = Object.keys(columns).map((key) => {
        let col = columns[key];
        if (typeof col === 'string') col = { type: col };

        let name = key;
        if (Array.isArray(columns)) name = col.name;
        if (col.column_type) {
          invalidColumns.push({ ...col, name, error: 'column_type is reserved for sql dialect' });
        }
        const typeDetails = this.dialect.getType(col.type) || {};
        try {
          return {
            ...SQLWorker.defaultStandardColumn, ...typeDetails, ...col, name,
          };
        } catch (e) {
          invalidColumns.push({ ...col, name, error: e });
          return null;
        }
      }).filter(Boolean);
      if (invalidColumns.length > 0) {
        invalidTables.push({ ...table }, { invalidColumns });
        return false;
      }
      table.indexes = (table.indexes || []).map((d) => ({
        columns: (typeof d.columns === 'string') ? d.columns.split(',').map((x) => x.trim()) : d.columns,
        primary: d.primary || false,
        unique: d.unique || d.primary || false,
      }));
      return table;
    });
    return standardSchema;
  } catch (e) {
    debug('Invalid parsed schema:', schema);
    throw e;
  }
};
Worker.prototype.standardize.metadata = {
  options: {
    schema: { description: 'Schema object,file path, or @engine9-interfaces/<interface_name>' },
  },
};

Worker.prototype.diff = async function (opts) {
  const schema = await this.standardize(opts);
  const { prefix = '' } = opts;
  if (prefix && prefix.slice(-1) !== '_') throw new Error(`A prefix should end with '_', it is ${prefix}`);
  const diffTables = await Promise.all(
    schema.tables.map(async (tableDefinition) => {
      const { name: table, columns: schemaColumns, indexes: schemaIndexes } = tableDefinition;
      debug(`Checking table ${table}`);
      let desc = null;
      try {
        desc = await this.describe({ table: prefix + table });
      } catch (e) {
        if (e?.code === 'DOES_NOT_EXIST') {
          desc = { columns: [], indexes: [] };
          tableDefinition.differences = ['missing'];
          return tableDefinition;

          /* return {table, differences: 'missing',
           columns: schemaColumns, indexes: schemaIndexes,};
           */
        }
        throw e;
      }
      if (!desc.columns) {
        debug(desc);
        throw new Error('No columns in describe table');
      }

      const indexes = await this.indexes({ table: prefix + table });
      const missingIndexes = schemaIndexes.filter((x) => !indexes.find((tableIndex) => {
        if (x.unique !== tableIndex.unique) return false;
        if (!Array.isArray(x.columns)) throw new Error('Non-array columns in indexes', schema);
        if (x.columns.join() !== tableIndex.columns.join()) return false;
        return true;
      }));

      const dbLookup = desc.columns.reduce((o, col) => Object.assign(o, { [col.name]: col }), {});
      debug(dbLookup);
      const columnDifferences = schemaColumns.map((c) => {
        const dbColumn = dbLookup[c.name];
        debug(table, c.name, dbColumn);
        if (!dbColumn) return { differences: 'new', ...c };
        // legacy hack -- don't change this column by hand
        if (c.name === 'source_code_id') return null;
        const differenceKeys = Object.keys(c).reduce((out, k) => {
          // Ignore these attributes
          if (['type', 'description', 'knex_args', 'values'].indexOf(k) >= 0) return out;
          // enum lengths are not really standardized
          if (c.type === 'enum' && k === 'length') return out;
          // json lengths aren't standard
          if (c.type === 'json' && k === 'length') return out;
          if (k === 'default_value') {
            // databases coerce undefined to NULL default values, this accounts for that
            if (dbColumn[k] === null && c[k] === undefined) {
              return out;
            }
          }
          if (c[k] !== dbColumn[k]) {
            // debug(c.name, k, c[k], '!=', dbColumn[k]);
            out[k] = { schema: c[k], db: dbColumn[k] };
          }
          return out;
        }, {});
        if (Object.keys(differenceKeys).length > 0) {
          return { differences: differenceKeys, ...c };
        }
        return null;
      }).filter(Boolean);

      const out = { name: table, differences: [] };
      if (columnDifferences.length > 0) {
        out.differences.push('columns');
        out.columns = columnDifferences;
      }
      if (missingIndexes.length > 0) {
        out.differences.push('indexes');
        out.indexes = missingIndexes;
      }
      if (out.differences.length === 0) return null;

      return out;
    }),
  );
  const tables = diffTables.filter(Boolean);
  debug(`Returning ${tables.length} diff tables for schema ${opts.schema}`);
  return { tables };
};

Worker.prototype.diff.metadata = {
  options: {
    table: {},
    schema: { description: 'Either a schema object, or a path to a schema file' },
  },
};

Worker.prototype.deploy = async function (opts) {
  const worker = this;
  const { tables } = await this.diff(opts);
  if (tables.length === 0) return { no_changes: true };
  const { prefix = '' } = opts;
  debug(`Deploying ${tables.length} tables, including`, JSON.stringify(tables[0], null, 4));
  async function processTable(tableDefinition) {
    const {
      name: table, type, differences, columns = [], indexes = [],
    } = tableDefinition;
    if (!table) {
      debug(tableDefinition);
      throw new Error('Invalid definition of table, no name');
    }
    const diffs = Array.isArray(differences) ? differences : [differences];
    const diffResults = await Promise.all(
      diffs.map(async (difference) => {
        if (difference === 'missing') {
          if (type === 'view') {
            return worker.createView(tableDefinition);
          }
          debug(`Creating table ${prefix}${table}`);
          return worker.createTable({ table: prefix + table, columns, indexes });
        }
        // Okay, it's not missing
        if (difference === 'columns') {
          const databaseType = await worker.tableType({ table: prefix + table });
          if (databaseType === 'view') return { name: table, difference, did_nothing_because_view: true };
          debug(`Altering table ${prefix}${table} with difference ${difference}`);
          return worker.alterTable({ table: prefix + table, columns });
        }
        if (difference === 'indexes') {
          const databaseType = await worker.tableType({ table: prefix + table });
          if (databaseType === 'view') return { name: table, difference, did_nothing_because_view: true };
          debug(`Altering table ${prefix}${table} with difference ${difference}`);
          return worker.alterTable({ table: prefix + table, indexes });
        }

        return { table, difference, did_nothing: true };
      }),
    );
    return diffResults;
  }

  const output = await Promise.all(
    tables.filter((d) => d.type !== 'view').map(processTable),
  );
  const views = await Promise.all(
    tables.filter((d) => d.type === 'view').map(processTable),
  );

  return { tables: output.concat(views) };
};
Worker.prototype.deploy.metadata = {
  options: {
    schema: { description: 'Either a schema object, or a path to a schema file' },
  },
};

Worker.prototype.deployStandard = async function deploy() {
  const schemaWorker = this;

  debug('Deploying schemas');
  await schemaWorker.deploy({ schema: 'engine9-interfaces/person' });
  await schemaWorker.deploy({ schema: 'engine9-interfaces/person_remote' });
  await schemaWorker.deploy({ schema: 'engine9-interfaces/person_email' });
  await schemaWorker.deploy({ schema: 'engine9-interfaces/person_phone' });
  await schemaWorker.deploy({ schema: 'engine9-interfaces/person_address' });
  await schemaWorker.deploy({ schema: 'engine9-interfaces/plugin' });
  await schemaWorker.deploy({ schema: 'engine9-interfaces/timeline' });
  await schemaWorker.deploy({ schema: 'engine9-interfaces/source_code' });
  await schemaWorker.deploy({ schema: 'engine9-interfaces/transaction' });
  await schemaWorker.deploy({ schema: 'engine9-interfaces/message' });
  debug('Deployed all schemas');

  schemaWorker.destroy();
  return { complete: true };
};
Worker.prototype.deployStandard.metadata = {
};

module.exports = Worker;
