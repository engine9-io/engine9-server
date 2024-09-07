/* eslint-disable no-console */
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

const connectionConfig = require('../../account-config.json');

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
  const { accountId } = req;

  let databaseWorker = databaseWorkerCache.get(accountId);

  if (!databaseWorker) {
    const config = knexConfigForTenant(accountId);

    const knex = Knex(config);
    databaseWorker = new SQLWorker({ accountId, knex });
    databaseWorkerCache.set(accountId, databaseWorker);
  }

  return databaseWorker;
}

// Validate permissions
router.use((req, res, next) => {
  req.accountId = req.get('X-ENGINE9-ACCOUNT-ID');
  if (!req.accountId) return res.status(401).json({ error: 'No X-ENGINE9-ACCOUNT-ID header' });
  const { user } = req;
  if (!user) {
    return res.status(401).json({ error: 'No valid user' });
  }

  /*
  const hasUser = connectionConfig.accounts?.[req.accountId]?.userIds?.find((d) => d === userId);
  if (hasUser) return next();

  //Currently only supporter firebase user ids, expand at some point later
  */

  const userId = user.uid;
  if (userId) {
    const isAdmin = connectionConfig
      .adminUserIds?.find((d) => d === userId);
    if (isAdmin) {
      return next();
    }
    const firebaseAuthed = connectionConfig
      .accounts?.[req.accountId]?.userIds?.find((d) => d === userId);
    if (firebaseAuthed) {
      return next();
    }
    console.error('User unauthorized: No firebaseUserId found for', user);
    return res.status(401).json({ error: `User not authorized for account ${req.accountId}` });
  }
  console.error('User unauthorized: No firebaseUserId for user:', user);
  return res.status(401).json({ error: 'User unauthorized' });
});

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
const uuidMatch = /^[0-9,a-f]{8}-[0-9,a-f]{4}-[0-9,a-f]{4}-[0-9,a-f]{4}-[0-9,a-f]{12}$/;
async function getData(options, databaseWorker) {
  const { table, limit = 100, offset = 0 } = options;
  if (!table) throw new ObjectError({ code: 422, message: 'No table provided' });
  // Limit include calls to not beat up the DB
  if (options.includes && !options.id && parseInt(limit, 10) > 100) {
    throw new ObjectError({ code: 422, message: 'No includes allowed with limits>100.  Please adjust the limit' });
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
    let id = parseInt(options.id, 10);
    if (uuidMatch.test(options.id)) id = `'${options.id}'`;
    else if (Number.isNaN(id)) throw new Error('Invalid id');
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
    data = r.data.map(({ id, ...attributes }) => ({
      type: table,
      id,
      attributes,
    }));
  } catch (e) {
    debug(e);
    throw new ObjectError({ status: 422, message: 'Invalid SQL was generated.' });
  }
  // If there's no includes, just return the data
  if (!options.include) return { data };

  let includeOption = null;
  try {
    includeOption = JSON5.parse(options.include);
  } catch (e) {
    debug(options.include, e);
    throw new ObjectError({ status: 422, message: 'Invalid JSON5 for includes' });
  }
  if (Array.isArray(includeOption)) {
    throw new ObjectError({
      code: 422,
      message: 'Invalid include, use an object not an array',
    });
  } else {
    // Turn it into an array if it's not
    includeOption = Object.keys(includeOption).map((property) => {
      if (!includeOption[property]) return false;
      if (includeOption[property].property) throw new ObjectError({ status: 422, message: `Invalid include property ${property}.property, do not include 'property' in a non-array includes object` });
      includeOption[property].property = property;
      return includeOption[property];
    }).filter(Boolean);
  }
  const dataLookup = {};
  data.forEach((d) => { dataLookup[d.id] = d; });
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
  // Still parse the includes, we still want to throw an invalid includes
  // even if there's no data
  const cleanedIncludes = includeOption.map((inc) => {
    if (inc.includes) throw new ObjectError({ status: 422, message: `Invalid include property ${inc.property}. Includes cannot have sub-includes.` });
    // Add in the primary key filter
    inc.conditions = makeArray(inc.conditions);
    inc.foreign_id_field = inc.foreign_id_field || `${table}_id`;
    inc.conditions.push({
      eql: `${inc.foreign_id_field} in (${allIds.map((id) => {
        if (uuidMatch.test(id)) return `'${options.id}'`;
        if (Number.isNaN(id)) throw new Error('Invalid id');
        return `${id}`;
      }).join(',')})`,
    });
    inc.limit = allIds.length * 25; // limit to 25x the number of records

    // prefill empty data
    data.forEach((d) => { d[inc.property] = []; });
    return inc;
  });
  /*
    Here we've validated the includes, but don't bother running them
  as there's no primary data */
  if (allIds.length === 0) {
    return data;
  }

  const includeData = await Promise.all(
    cleanedIncludes.map((inc) => {
      debug('Running include for ids:', allIds, inc);
      return getData(inc, databaseWorker);
    }),
  );
  let includes = [];
  includeData.forEach((includeResults, i) => {
    const inc = cleanedIncludes[i];
    includes = includes.concat(includeResults.data.map((r) => {
      if (!inc.foreign_id_field) {
        throw new ObjectError('no foreign_id_field');
      }
      const referenceId = r.attributes[inc.foreign_id_field];
      if (referenceId === undefined) {
        throw new Error(`Could not find id field ${inc.foreign_id_field} in included result object:${JSON.stringify(r)}`);
      }
      if (!dataLookup[referenceId]) {
        console.error(`Could not find referenceId ${referenceId} from field ${inc.foreign_id_field} in lookup ids of length:${Object.keys(dataLookup).length}`, ' sample:', Object.keys(dataLookup).slice(0, 30));
        throw new Error(`Error merging includes:, could not find referenceId ${referenceId} from field ${inc.foreign_id_field}`);
      }
      dataLookup[referenceId][inc.property] = dataLookup[referenceId][inc.property] || { data: [] };
      dataLookup[referenceId][inc.property].push({ type: inc.table, id: r.id });
      const { id, attributes } = r;
      return {
        type: inc.table,
        id,
        attributes,
      };
    }));
  });
  return { data, includes };
}

router.get([
  '/tables/:table/:id',
  '/tables/:table'], async (req, res) => {
  const start = new Date().getTime();
  console.log('Starting get data');
  try {
    const table = req.params?.table;
    if (!table) throw new ObjectError({ code: 422, message: 'No table provided in the uri' });

    const output = await getData({
      table, id: req.params?.id, ...req.query,
    }, req.databaseWorker);
    if (req.query.schema === 'true') {
      output.schema = await req.databaseWorker.describe({ table: req.params.table });
    }
    console.log(`Returning in ${new Date().getTime() - start}`);
    return res.json(output);
  } catch (e) {
    debug('Error handling request:', e, e.code, e.message);
    return res.status(e.status || 500).json({ message: e.message || 'Error with request' });
  }
});

router.post([
  '/message/:message_id',
  '/message'], async (req, res) => {
  try {
    const { body } = req;
    if (req.params?.message_id) body.id = req.params?.message_id;
    const r = await req.databaseWorker.upsertMessage(body);
    return res.json(r);
  } catch (e) {
    debug('Error handling request:', e, e.code, e.message);
    return res.status(e.status || 500).json({ errors: [{ message: e.message || 'Error with request' }] });
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
      body.id = id;
      await req.databaseWorker.updateOne({ table, data: body });
    } else {
      const response = await req.databaseWorker.insertOne({ table, data: body });

      [id] = response;
    }

    return res.json({ id });
  } catch (e) {
    debug('Error handling request:', e, e.code, e.message);
    return res.status(e.status || 500).json({ errors: [{ message: e.message || 'Error with request' }] });
  }
});

router.post(['/eql'], async (req, res) => {
  try {
    const { body } = req;
    debug('Body=', body);
    const output = await getData(body, req.databaseWorker);
    return res.json(output);
  } catch (e) {
    debug('Error handling request:', e, e.code, e.message);
    return res.status(e.status || 500).json({ errors: [{ message: e.message || 'Error with request' }] });
  }
});
router.get('/query/fields', async (req, res) => {
  res.json({
    fields: [
      {
        name: 'given_name',
        label: 'First Name',
        placeholder: 'Enter first name',
      },
      {
        name: 'family_name',
        label: 'Last Name',
        placeholder: 'Enter last name',
        defaultOperator: 'beginsWith',
      },
      {
        name: 'age', label: 'Age', inputType: 'number',
      },
      {
        name: 'segment',
        label: 'Segment',
        valueEditorType: 'select',
        values: [
          {
            label: 'Static',
            options: [
              { name: 1, label: 'Segment 1' },
              { name: 2, label: 'Segment 2' },
            ],
          },
          {
            label: 'Dynamic',
            options: [
              { name: 3, label: 'Segment 3' },
              { name: 4, label: 'Segment 4' },
            ],
          },
        ],
        operators: [
          { name: 'in', value: 'in', label: 'Is a member of' },
          { name: 'notIn', value: 'notIn', label: 'Is not a member of' },
        ],
        defaultValue: false,
      },

      {
        name: 'gender',
        label: 'Gender',
        operators: ['='],
        valueEditorType: 'radio',
        values: [
          { name: 'M', label: 'Male' },
          { name: 'F', label: 'Female' },
          { name: 'O', label: 'Other' },
        ],
      },
      { name: 'height', label: 'Height' },
      { name: 'job', label: 'Job' },
      { name: 'description', label: 'Description', valueEditorType: 'textarea' },
      { name: 'birthdate', label: 'Birth Date', inputType: 'date' },
      { name: 'datetime', label: 'Show Time', inputType: 'datetime-local' },
      { name: 'alarm', label: 'Daily Alarm', inputType: 'time' },
    ],
  });
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