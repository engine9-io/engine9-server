require('dotenv').config({ path: '../../.env' });
const express = require('express');
const debug = require('debug')('job-api');
const bodyParser = require('body-parser');

const router = express.Router({ mergeParams: true });

router.get('/jobs', async (req, res) => {
  const { accountId } = req.user || {};
  // if (!accountId) return res.status(401).json({ message: 'No accountId' });
  if (req.query?.test) {
    return res.json({
      data: [
        {
          type: 'job',
          id: 'test',
          attributes: {
            accountId,
            label: 'Test Job',
          },
        },
      ],
    });
  }
  return res.json({ data: [] });
});

router.patch('/jobs/:jobId', (req, res) => {
  const { jobId } = req.params;
  const { body } = req;
  debug(`Patching ${jobId} with `, body);
  return res.status(200).json({
    meta: { ok: true },
    data: {
      type: 'job',
      id: jobId,
    },
  });
});

if (require.main === module) {
  const url = new URL(process.env.ENGINE9_JOB_API_URL || 'http://localhost');
  const port = url.port || 80;
  const app = express();

  app.use(bodyParser.json());
  app.use(bodyParser.urlencoded({ extended: true }));
  app.use(router);

  // const httpServer = http.createServer(app);
  app.listen(port, (e) => {
    if (e) throw e;
    debug('listening insecurely on:', port);
  });
}

module.exports = router;
