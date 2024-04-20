const { execFile } = require('node:child_process');

const express = require('express');
const debug = require('debug')('packet-server');
const JSON5 = require('json5');

const router = express.Router({ mergeParams: true });

router.get('/', async (req, res) => {
  let { accountId } = req.user || {};

  // REMOVE ME
  if (!accountId) accountId = 'authentic_azdems';

  let { path } = req.query;
  const { file } = req.query;

  if (path.indexOf('/') === 0) path = path.slice(1);
  const packetPath = `s3://engine9-accounts/${accountId}/${path}`;

  let formatter = (c) => c;
  let args = ['ls', packetPath];
  if (file === 'info') {
    args = ['info', packetPath];
  } else if (file) {
    args = ['cat', packetPath, file];
    if (file.split('.').pop().indexOf('js') === 0) {
      formatter = (c) => JSON5.parse(c);
    }
  } else {
    formatter = (c) => ({
      files: c.trim().split('\n').filter(Boolean).map((f) => {
        const [permissions, compressed, uncompressed, created, filepath] = f.split('\t');
        debug({
          permissions, compressed, uncompressed, created, filepath,
        });
        return {
          compressed: parseInt(compressed.trim(), 10),
          uncompressed: parseInt(uncompressed.trim(), 10),
          created: new Date(created).toISOString(),
          file: filepath,
        };
      }),
    });
  }
  /* callback is called AFTER completion, so it's not streaming
  */
  debug('Using args', args);
  execFile('/usr/local/bin/cz', args, (error, stdout, stderr) => {
    if (error) {
      debug(error);
      return res.status(500).json('Error with packet');
    }
    debug(stderr);// log stderr
    try {
      const o = formatter(stdout);
      if (typeof o === 'object') {
        return res.status(200).json(o);
      }
      return res.status(200).send(o);
    } catch (e) {
      debug('Error formatting content', e);
      return res.status(200).send(stdout);
    }
  });

  // return res.json(content);
});

module.exports = router;
