// A lot of libraries initialize using the process.env object, so keep this first
require('dotenv').config({ path: '../.env' });
const debug = require('debug')('log-server');
const path = require('node:path');
const fs = require('node:fs');

const fsp = fs.promises;

const express = require('express');

const router = express.Router({ mergeParams: true });

router.get('/job', async (req, res) => {
  const accountId = req.query?.accountId || req.accountId || 'engine9';
  const { user } = req;
  if (!user.accounts?.[accountId]) {
    return res.status(401).json({ error: `No access to account ${accountId}` });
  }
  if (!user.accounts?.[accountId]?.level === 'admin') {
    return res.status(401).json({ error: `Insufficient privileges for account ${accountId}` });
  }
  const { jobId } = req.query;
  if (!jobId) return res.status(400).json({ error: 'No jobId specified' });
  const filename = path.resolve(`${process.env.ENGINE9_LOG_DIR}/jobs/${accountId}_${jobId}.txt`);
  // make sure no relative references are passed in
  if (filename.indexOf(`${process.env.ENGINE9_LOG_DIR}/jobs/${accountId}`) !== 0) {
    if (!jobId) return res.status(400).json({ error: 'Invalid account or job' });
  }

  // Defaults are the first 50K of a file
  const start = parseInt(req.query.start || 0, 10);
  let end = parseInt(req.query.end || 50000, 10);

  const stat = await fsp.stat(filename);

  const chunks = [];
  const opts = { start };
  if (start < 0) {
    opts.start = stat.size + start;
    if (opts.start < 0) opts.start = 0;
    // if (negative start, then go to the end of the file)
    end = null;
  }

  if (end) {
    if (end < 0) end = stat.size + end;
    else if (end > stat.size) end = stat.size;
    opts.end = end;
  }

  debug(`Calling read stream on file of ${stat.size} bytes with`, opts);
  const finalBuff = await new Promise((resolve, reject) => {
    const s = fs.createReadStream(filename, opts);
    s.on('data', (chunk) => {
      chunks.push(chunk);
    });
    s.on('error', (e) => {
      debug('Error with stream', e);
      reject(e);
    });

    s.on('end', () => {
      debug('Calling end on the buffer');
      resolve(Buffer.concat(chunks));
    });
  });
  res.set('Content-Type', 'text/plain');
  return res.send(finalBuff);
});

module.exports = router;
