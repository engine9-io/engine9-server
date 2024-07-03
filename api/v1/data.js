// A lot of libraries initialize using the process.env object, so keep this first
require('dotenv').config({ path: '../.env' });

process.env.DEBUG = '*';
const debug = require('debug')('api');

const express = require('express');
const JSON5 = require('json5');// Useful for parsing extended JSON

// knex starts up it's own debugger,
const Knex = require('knex');

const SQLWorker = require('../../workers/SQLWorker');
const { ObjectError } = require('../../utilities');

const router = express.Router({ mergeParams: true });

const databaseWorkerCache = new Map();

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

function getSQLWorkerForRequest(req) {
  // the account_id comes from authentication step, or in the headers,
  // NOT the url parameters, engine9 is the default
  const headerAccountId = req.get('ENGINE9_ACCOUNT_ID');
  const accountId = headerAccountId || 'engine9';

  let databaseWorker = databaseWorkerCache.get(accountId);

  if (!databaseWorker) {
    let config = null;
    try {
      config = knexConfigForTenant(accountId);
    } catch (e) {
      if (!headerAccountId) throw new Error('No ENGINE9_ACCOUNT_ID header');
      throw e;
    }

    const knex = Knex(config);
    databaseWorker = new SQLWorker({ accountId, knex });
    databaseWorkerCache.set(accountId, databaseWorker);
  }

  return databaseWorker;
}

router.use((req, res, next) => {
  req.databaseWorker = getSQLWorkerForRequest(req);
  next();
});

router.get('/ok', (req, res) => {
  res.json({ ok: true });
});

router.get('/tables/describe/:table', async (req, res) => {
  try {
    const desc = await req.databaseWorker.describe({ table: req.params.table });
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

async function getData(options, databaseWorker) {
  const { table, limit = 100, offset = 0 } = options;
  if (!table) throw new ObjectError({ code: 422, message: 'No table provided' });
  // Limit extension calls to not beat up the DB
  if (options.extensions && !options.id && parseInt(limit, 10) > 100) {
    throw new ObjectError({ code: 422, message: 'No extensions allowed with limits>100.  Please adjust the limit' });
  }

  const eqlObject = {
    table,
    limit,
    offset,
  };

  const invalid = ['conditions', 'groupBy', 'columns'].filter((k) => {
    try {
      eqlObject[k] = makeArray(options[k]);
      return false;
    } catch (e) {
      debug(`Error with ${k}`, typeof options[k], options[k], e);
      return e;
    }
  });

  if (invalid.length > 0) {
    throw new ObjectError({
      status: 422,
      message: `Unparseable values:${invalid.join()}`,
    });
  }
  // Not positive default to all is a great idea
  if (eqlObject.columns.length === 0)eqlObject.columns = ['*'];

  if (options.id) {
    const id = parseInt(options.id, 10);
    if (Number.isNaN(id)) throw new Error('Invalid id');
    eqlObject.conditions = (eqlObject.conditions || []).concat({ eql: `id=${id}` });
    eqlObject.limit = 0;
    delete eqlObject.offset;
  }

  let sql;
  try {
    sql = await databaseWorker.buildSqlFromEQLObject(eqlObject);
    debug('Retrieved sql', JSON.stringify({ eqlObject, sql }, null, 4));
  } catch (e) {
    debug('Error generating sql with object:', eqlObject);
    debug(e);
    throw new ObjectError({ status: 422, message: 'Error generating sql' });
  }
  let data = null;
  try {
    const r = await databaseWorker.query(sql);
    data = r.data;
  } catch (e) {
    debug(e);
    throw new ObjectError({ status: 422, message: 'Invalid SQL was generated.' });
  }
  // If there's no extensions, just return the data
  if (!options.extensions) return data;

  let extOption = null;
  try {
    extOption = JSON5.parse(options.extensions);
  } catch (e) {
    debug(e);
    throw new ObjectError({ status: 422, message: 'Invalid JSON5 for extensions' });
  }
  if (Array.isArray(extOption)) {
    throw new ObjectError({
      code: 422,
      message: 'Invalid extensions, use an object not an array',
    });
  } else {
    // Turn it into an array if it's not
    extOption = Object.keys(extOption).map((property) => {
      if (!extOption[property]) return false;
      if (extOption[property].property) throw new ObjectError({ status: 422, message: `Invalid extension property ${property}.property, do not include 'property' in a non-array extensions object` });
      extOption[property].property = property;
      return extOption[property];
    }).filter(Boolean);
  }
  const dataLookup = {};
  data.forEach((d) => { dataLookup[parseInt(d.id, 10)] = d; });
  const allIds = data.map((d) => {
    if (Number.isNaN(d.id)) {
      throw new ObjectError({
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
    if (ext.extensions) throw new ObjectError({ status: 422, message: `Invalid extension property ${ext.property}. Extensions cannot have sub-extensions.` });
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
      return getData(ext, databaseWorker);
    }),
  );
  extensionData.forEach((extensionResults, i) => {
    const ext = cleanedExtensions[i];
    extensionResults.forEach((r) => {
      if (!ext.foreign_id_field) {
        throw new ObjectError('no foreign_id_field');
      }
      const id = r[ext.foreign_id_field];
      dataLookup[id]?.[ext.property]?.push(r);
    });
  });
  return data;
}

router.get([
  '/tables/:table/:id',
  '/tables/:table'], async (req, res) => {
  try {
    const table = req.params?.table;
    if (!table) throw new ObjectError({ code: 422, message: 'No table provided in the uri' });
    const data = await getData({
      table, id: req.params?.id, ...req.query,
    }, req.databaseWorker);
    return res.json({ data });
  } catch (e) {
    debug('Error handling request:', e, e.code, e.message);
    return res.status(e.status || 500).json({ message: e.message || 'Error with request' });
  }
});

router.post([
  '/tables/:table/:id',
  '/tables/:table'], async (req, res) => {
  try {
    const table = req.params?.table;
    if (!table) throw new ObjectError({ code: 422, message: 'No table provided in the uri' });
    let id = req.params?.id;
    const { body } = req;
    if (id) {
      await req.databaseWorker.knex.table(table).insert(body);
    } else {
      const response = await req.databaseWorker.knex.table(table).insert(body);

      [id] = response;
    }

    return res.json({ id });
  } catch (e) {
    debug('Error handling request:', e, e.code, e.message);
    return res.status(e.status || 500).json({ message: e.message || 'Error with request' });
  }
});

router.post(['/eql'], async (req, res) => {
  try {
    const { body } = req;
    debug('Body=', body);
    const data = await getData(body, req.databaseWorker);
    return res.json({ data });
  } catch (e) {
    debug('Error handling request:', e, e.code, e.message);
    return res.status(e.status || 500).json({ message: e.message || 'Error with request' });
  }
});

/*
router.post(['/tables/:table/:id'], async (req, res) => {
  try {
    const table = req.params?.table;
    if (!table) throw new ObjectError({ code: 422, message: 'No table provided in the uri' });
    const data = await saveData({
      table, id: req.params?.id, ...req.query,
    }, req.databaseWorker);
    return res.json({ data });
  } catch (e) {
    debug('Error handling request:', e, e.code, e.message);
    return res.status(e.status || 500).json({ message: e.message || 'Error with request' });
  }
});
*/

module.exports = router;
