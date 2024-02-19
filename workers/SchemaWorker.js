/*
  Schema worker extends the SQLWorker to specifically work with DDL and schema definitions,
  as opposed to DML work
*/
const util = require('util');
const debug = require('debug')('SQLWorker');
const fs = require('fs');
const JSON5 = require('json5');// Useful for parsing extended JSON
const SQLWorker = require('./SQLWorker');
const SQLTypes = require('./SQLTypes');

function Worker(worker) {
  SQLWorker.call(this, worker);
}

util.inherits(Worker, SQLWorker);
Worker.metadata = {
  alias: 'schema',
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
  } else {
    const content = await fs.promises.readFile(_schema);
    if (!content) throw new Error(`No content found for ${_schema}`);
    try {
      schema = JSON5.parse(content);
    } catch (error) {
      throw new Error(`Invalid content at ${_schema}, ${error.message}`);
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
        const typeDetails = SQLTypes.getType(col.type) || {};
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
    schema: { description: 'Schema object or file path' },
  },
};

Worker.prototype.diff = async function (opts) {
  const schema = await this.standardize(opts);
  const { prefix = '' } = opts;
  if (prefix && prefix.slice(-1) !== '_') throw new Error(`A prefix should end with '_', it is ${prefix}`);
  const diffTables = await Promise.all(
    schema.tables.map(async ({ name: table, columns: schemaColumns }) => {
      let desc = null;
      try {
        desc = await this.describe({ table: prefix + table });
      } catch (e) {
        if (e?.cause === 'DOES_NOT_EXIST') {
          desc = { columns: [], indexes: [] };
          return { table, columns: schemaColumns, exists: false };
        }
        throw e;
      }
      if (!desc.columns) {
        debug(desc);
        throw new Error('No columns in describe table');
      }

      const dbLookup = desc.columns.reduce((o, col) => Object.assign(o, { [col.name]: col }), {});

      const differences = schemaColumns.map((c) => {
        const dbColumn = dbLookup[c.name];
        if (!dbColumn) return { differences: 'new', ...c };
        const differenceKeys = Object.keys(c).reduce((out, k) => {
          if (k === 'type') return out;
          if (c[k] !== dbColumn[k]) {
            debug(c.name, k, c[k], '!=', dbColumn[k]);
            out[k] = { schema: c[k], db: dbColumn[k] };
          }
          return out;
        }, {});
        if (Object.keys(differenceKeys).length > 0) {
          return { differences: differenceKeys, ...c };
        }
        return null;
      }).filter(Boolean);
      if (differences.length === 0) return null;
      return { table, columns: differences };
    }),
  );

  return { tables: diffTables.filter(Boolean) };
};

Worker.prototype.diff.metadata = {
  options: {
    table: {},
    schema: { description: 'Either a schema object, or a path to a schema file' },
  },
};

Worker.prototype.deploy = async function (opts) {
  const { tables } = await this.diff(opts);
  if (tables.length === 0) return { no_changes: true };
  const { prefix = '' } = opts;
  debug(`Creating ${tables.length} tables, including`, tables[0]);
  await Promise.all(
    tables.map(async ({ table, columns }) => {
      debug(`Creating table ${prefix}${table}`);
      return this.createTable({ table: prefix + table, columns });
    }),
  );

  return { tables };
};
Worker.prototype.deploy.metadata = {
  options: {
    schema: { description: 'Either a schema object, or a path to a schema file' },
  },
};

module.exports = Worker;
