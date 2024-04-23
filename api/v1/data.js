// A lot of libraries initialize using the process.env object, so keep this first
require('dotenv').config({ path: '../.env' });

process.env.DEBUG = '*';
const debug = require('debug')('api');

const express = require('express');
const JSON5 = require('json5');// Useful for parsing extended JSON

// knex starts up it's own debugger,
const Knex = require('knex');

const QueryWorker = require('../../workers/QueryWorker');
const { ErrorObject } = require('../../utilities');

const router = express.Router({ mergeParams: true });

const queryWorkerCache = new Map();

let connectionConfig = null;

try {
  // eslint-disable-next-line global-require
  connectionConfig = require('../../account-config.json');
} catch (e) {
  debug(e);
  throw new Error('Error loading config.json file -- make sure to create one from config.template.json before running');
}

function knexConfigForTenant(accountId) {
  if (!accountId) throw new Error('accountId is required');
  const connection = connectionConfig.accounts?.[accountId]?.auth?.database_connection;
  if (!connection) throw new Error(`No connection configuration for account ${accountId}`);
  const parts = connection.split(':');
  switch (parts[0]) {
    case 'mysql': return {
      client: 'mysql2',
      connection,
    };
    default: throw new Error(`Unsupported database: ${parts[0]}`);
  }
}

function getQueryWorkerForRequest(req) {
  // the account_id comes from authentication step, or in the headers,
  // NOT the url parameters, engine9 is the default
  const headerAccountId = req.get('ENGINE9_ACCOUNT_ID');
  const accountId = headerAccountId || 'engine9';

  let queryWorker = queryWorkerCache.get(accountId);

  if (!queryWorker) {
    let config = null;
    try {
      config = knexConfigForTenant(accountId);
    } catch (e) {
      if (!headerAccountId) throw new Error('No ENGINE9_ACCOUNT_ID header');
      throw e;
    }

    const knex = Knex(config);
    queryWorker = new QueryWorker({ accountId, knex });
    queryWorkerCache.set(accountId, queryWorker);
  }

  return queryWorker;
}

router.use((req, res, next) => {
  req.queryWorker = getQueryWorkerForRequest(req);
  next();
});

router.get('/ok', (req, res) => {
  res.json({ ok: true });
});

router.get('/tables/describe/:table', async (req, res) => {
  try {
    const desc = await req.queryWorker.describe({ table: req.params.table });
    return res.json(desc);
  } catch (e) {
    return res.status(422).json({ error: 'Invalid table' });
  }
});

function makeArray(s) {
  if (!s) return [];
  if (typeof s === 'object') {
    if (Array.isArray(s)) return s;
    return [s];
  }

  const obj = JSON5.parse(s);
  if (Array.isArray(obj)) return obj;
  return [obj];
}

async function getData(options, queryWorker) {
  const { table, limit = 100, offset = 0 } = options;
  if (!table) throw new ErrorObject({ code: 422, message: 'No table provided' });
  if (options.extensions && !options.id) throw new ErrorObject({ code: 422, message: 'No table provided' });
  const query = {
    table,
    limit,
    offset,
  };

  const invalid = ['conditions', 'group_by', 'columns'].filter((k) => {
    try {
      query[k] = makeArray(options[k]);
      return false;
    } catch (e) {
      debug(`Error with ${k}`, typeof options[k], options[k], e);
      return e;
    }
  });

  if (invalid.length > 0) {
    throw new ErrorObject({
      status: 422,
      message: `Unparseable query values:${invalid.join()}`,
    });
  }
  // Not positive default to all is a great idea
  if (query.columns.length === 0)query.columns = ['*'];

  if (options.id) {
    const id = parseInt(options.id, 10);
    if (Number.isNaN(id)) throw new Error('Invalid id');
    query.conditions = (query.conditions || []).concat({ eql: `id=${id}` });
    query.limit = 0;
    delete query.offset;
  }

  let sql;
  try {
    sql = await queryWorker.buildSqlFromQueryObject(query);
    debug('Retrieved sql', JSON.stringify({ query, sql }, null, 4));
  } catch (e) {
    debug('Error generating query with columns:', query);
    debug(e);
    throw new ErrorObject({ status: 422, message: 'Error generating query' });
  }
  let data = null;
  try {
    const r = await queryWorker.query(sql);
    data = r.data;
  } catch (e) {
    debug(e);
    throw new ErrorObject({ status: 422, message: 'Invalid SQL was generated.' });
  }
  // If there's no extensions, just return the data
  if (!options.extensions) return data;

  let extOption = null;
  try {
    extOption = JSON5.parse(options.extensions);
  } catch (e) {
    debug(e);
    throw new ErrorObject({ status: 422, message: 'Invalid JSON5 for extensions' });
  }
  if (Array.isArray(extOption)) {
    // we're fine, it's an array not an object
  } else {
    // Turn it into an array if it's not
    extOption = Object.keys(extOption).map((property) => {
      if (!extOption[property]) return false;
      if (extOption[property].property) throw new ErrorObject({ status: 422, message: `Invalid extension property ${property}.property, do not include 'property' in a non-array extensions object` });
      extOption[property] = property;
      return extOption[property];
    }).filter(Boolean);
  }
  const dataLookup = {};
  data.forEach((d) => { dataLookup[parseInt(d.id, 10)] = d; });
  const allIds = data.map((d) => {
    if (Number.isNaN(d.id)) {
      throw new ErrorObject({
        code: 422,
        message: `An id that was not a number was returned:${d.id}`,
      });
    }
    return d.id;
  });
  debug('Retrieved Ids:', allIds);
  // Still parse the extensions, we still want to throw an invalid extensions
  // even if there's no data
  const cleanedExtensions = extOption.map((ext) => {
    if (ext.extensions) throw new ErrorObject({ status: 422, message: `Invalid extension property ${ext.property}. Extensions cannot have sub-extensions.` });
    // Add in the primary key filter
    ext.conditions = makeArray(ext.conditions);
    ext.foreign_id_field = ext.foreign_id_field || `${table}_id`;
    ext.conditions.push({
      eql: `${ext.foreign_id_field} in (${allIds.map((id) => `${id}`).join(',')})`,
    });
    ext.limit = allIds.length * 25; // limit to 25x the number of records

    // prefill empty data
    data.forEach((d) => { d[ext.property] = []; });
    return ext;
  });
  /*
    Here we've validated the extensions, but don't bother running them
  as there's no primary data */
  if (allIds.length === 0) {
    return data;
  }

  const extensionData = await Promise.all(
    cleanedExtensions.map((ext) => {
      debug('Running extension for ids:', allIds, ext);
      return getData(ext, queryWorker);
    }),
  );
  extensionData.forEach((extensionResults, i) => {
    const ext = cleanedExtensions[i];
    extensionResults.forEach((r) => {
      const id = r[ext.foreign_id_field];
      dataLookup[id]?.[ext.foreign_id_field]?.push(r);
    });
  });
  return data;
}

router.get([
  '/tables/:table/:id',
  '/tables/:table'], async (req, res) => {
  try {
    const data = await getData({
      table: req.params?.table, id: req.params?.id, ...req.query,
    }, req.queryWorker);
    return res.json({ data });
  } catch (e) {
    debug('Error handling request:', e, e.code, e.message);
    return res.status(e.status || 500).json({ message: e.message || 'Error with request' });
  }
});

module.exports = router;
