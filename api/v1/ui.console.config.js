// A lot of libraries initialize using the process.env object, so keep this first
require('dotenv').config({ path: '../.env' });

process.env.DEBUG = '*';
// const debug = require('debug')('api/ui');
const express = require('express');

const UIWorker = require('../../workers/UIWorker');

const router = express.Router({ mergeParams: true });

const uiWorker = new UIWorker();
router.get('/console', async (req, res) => {
  const config = await uiWorker.getConsoleConfig(
    { account_id: req.accountId, user_id: req.userId },
  );
  return res.json(config);
});

module.exports = router;
