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
  alias: 'sql',
};

/*
  Retrieves, validates, and expands a schema to a standard for the correct database.
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
  // handled elsewhere if they're included, this method is for database modifications
  try {
    const standardSchema = JSON.parse(JSON.stringify(schema));
    const invalidTables = [];
    standardSchema.tables = (standardSchema.tables || []).map((table) => {
      const invalidColumns = [];
      const columns = table.columns || [];
      table.columns = Object.keys(columns).map((key) => {
        const col = columns[key];
        let name = key;
        if (Array.isArray(columns)) name = col.name;
        try {
          return { ...SQLTypes.mysql.toColumn(col), name };
        } catch (e) {
          invalidColumns.push({ ...col, name });
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

Worker.prototype.diff = async function ({ table, _schema }) {
  const schema = await this.standardize({ schema: _schema });
  const desc = await this.describe({ table });

  const dbLookup = desc.columns.reduce((o, col) => Object.assign(o, { [col.name]: col }), {});

  let schemaArray = schema.columns;
  if (!Array.isArray(schema)) {
    schemaArray = Object.entries(schema)
      .reduce((a, [name, v]) => a.concat(Object.assign(v, { name })), []);
  }
  // const schemaLookup = schemaArray.reduce((o, col) => Object.assign(o, { [o.name]: o }, {}));
  const differences = schemaArray.map((c) => {
    const dbColumn = dbLookup[c.name];
    if (!dbColumn) return { differences: 'new', ...c };
    const differenceKeys = Object.keys(c).reduce((out, k) => {
      if (c[k] !== dbColumn[k]) {
        debug(c.name, k, c[k], '!=', dbColumn[k]);
        out[k] = c[k];
      }
      return out;
    }, {});
    if (Object.keys(differenceKeys).length > 0) {
      return { differences: differenceKeys, ...c };
    }
    return null;
  }).filter(Boolean);
  return { differences };
};

Worker.prototype.diff.metadata = {
  options: {
    table: {},
    schema: { description: 'Either a schema object, or a path to a schema file' },
  },
};

module.exports = Worker;
