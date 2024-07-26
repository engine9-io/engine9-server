// A lot of libraries initialize using the process.env object, so keep this first
require('dotenv').config({ path: '../.env' });

process.env.DEBUG = '*';
// const debug = require('debug')('api/ui');
const express = require('express');

const UIWorker = require('../../workers/UIWorker');

const router = express.Router({ mergeParams: true });

router.get('/console', async (req, res) => {
  const accountId = req.accountId || 'engine9'; // CHANGE ME
  const uiWorker = new UIWorker({ accountId });
  const config = await uiWorker.getConsoleConfig(
    { accountId, userId: req.userId },
  );
  return res.json(config);
});

module.exports = router;
