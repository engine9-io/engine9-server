// A lot of libraries initialize using the process.env object, so keep this first
require('dotenv').config({ path: '../.env' });

process.env.DEBUG = '*';
const debug = require('debug')('api');

const express = require('express');
const JSON5 = require('json5');// Useful for parsing extended JSON

// knex starts up it's own debugger,
const Knex = require('knex');

const QueryWorker = require('../../workers/QueryWorker');

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

  const obj = JSON5.parse(s);
  if (Array.isArray(obj)) return obj;
  return [obj];
}

router.get([
  '/tables/:table/:id',
  '/tables/:table'], async (req, res) => {
  const { table } = req.params;
  if (!table) throw new Error('No table provided');
  const { limit = 100, offset = 0 } = req.query;
  const query = {
    table,
    limit,
    offset,
  };

  const invalid = ['conditions', 'group_by', 'columns'].filter((k) => {
    try {
      query[k] = makeArray(req.query[k]);
      return false;
    } catch (e) {
      debug(e);
      return true;
    }
  });
  if (invalid.length > 0) {
    return res.status(422).json({ error: `Unparseable query values:${invalid.join()}` });
  }
  if (req.params.id) {
    const id = parseInt(req.params.id, 10);
    if (Number.isNaN(id)) throw new Error('Invalid id');
    query.conditions = (query.conditions || []).concat({ eql: `id=${id}` });
    query.limit = 0;
    delete query.offset;
  }

  let sql;
  try {
    sql = await req.queryWorker.buildSqlFromQueryObject(query);
  } catch (e) {
    debug('Error generating query with columns:', query);
    debug(e);
    return res.status(422).json({ error: 'Invalid query values' });
  }
  try {
    const { data } = await req.queryWorker.query(sql);
    return res.json({ data });
  } catch (e) {
    debug('Error running SQL:', { query, sql });
    return res.status(422).json({ error: 'SQL Error' });
  }
});

module.exports = router;
