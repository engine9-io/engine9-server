// A lot of libraries initialize using the process.env object, so keep this first
require('dotenv').config({ path: '../.env' });

process.env.DEBUG = '*';
// const debug = require('debug')('api/ui');
const express = require('express');
const JSON5 = require('json5');// Useful for parsing extended JSON
const fs = require('node:fs');

const fsp = fs.promises;
const router = express.Router({ mergeParams: true });

router.get('/demo', async (req, res) => {
  const content = JSON5.parse(await fsp.readFile('v1/ui-demo.json5'));
  return res.json(content);
});

module.exports = router;
