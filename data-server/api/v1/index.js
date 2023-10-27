const express = require('express');
const { Model } = require('objection');
const Knex = require('knex');

const router = express.Router({ mergeParams: true });

const knexCache = new Map();

require('dotenv').config({ path: '../.env' });

class Person extends Model {
  static get tableName() {
    return 'person';
  }
}
const modelMap = new Map();
modelMap.set('person', Person);

function knexConfigForTenant(accountId) {
  if (accountId === 'steamengine') {
    return {
      client: 'mysql2',
      connection: process.env.STEAMENGINE_DATABASE_CONNECTION_STRING,
    };
  }
  throw new Error(`Account ${accountId} not supported`);
}

function getKnexForRequest(req) {
  const accountId = req.params.account_id;
  let knex = knexCache.get(accountId);

  if (!knex) {
    knex = Knex(knexConfigForTenant(accountId));
    knexCache.set(accountId, knex);
  }

  return knex;
}

router.use((req, res, next) => {
  req.knex = getKnexForRequest(req);
  next();
});

router.get('/ok', (req, res) => {
  res.json({ ok: true, params: req.params });
});

router.get('/:object/:id', async (req, res) => {
  const object = await Person.query(req.knex).findById(req.params.id);
  return res.json(object);
});
router.get('/:object', async (req, res) => {
  const objectName = req.params.object;
  const model = modelMap.get(objectName);
  if (!model) return res.status(404).json({ error: `No such model: ${objectName}` });
  const objects = await model.query(req.knex).limit(100);
  return res.json(objects);
});

module.exports = router;
