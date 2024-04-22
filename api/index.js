const express = require('express');

const app = express();
const cors = require('cors');
const compression = require('compression');
const bodyParser = require('body-parser');

const isDevelopment = process.env.NODE_ENV === 'development';

const corsOptions = {
  origin: (origin, callback) => {
    if (!origin) return callback(null, true); // support direct calls, mostly for testing
    const url = new URL(origin);
    if (url.hostname === 'localhost' || url.hostname.endsWith('engine9.io')) {
      return callback(null, true);
    }
    return callback(new Error('Invalid CORS domain'));
  },
};
app.use(cors(corsOptions));

app.use(bodyParser.json());
if (isDevelopment) app.set('json spaces', 2); // number of spaces for indentation
app.use(bodyParser.urlencoded({ extended: true }));

const data = require('./v1/data');
const ui = require('./v1/ui-config');
const packetServer = require('./packet-server/index');

app.use(cors());
app.use(compression());
app.use(bodyParser.json());

app.get('/ok', (req, res) => { res.json({ ok: true }); });
app.use('/data', data);
app.use('/ui-config', ui);
app.use('/packet', packetServer);

/*
error handling -- default is a bit better
than this, handles NODE_ENV, etc
app.use((err, req, res, next) => {
  if (res.headersSent) {
    return next(err);
  }
  res.status(500);
  return res.send({ error: err });
});
*/

const port = 8080;

app.listen(port, (e) => {
  if (e) throw e;
  // eslint-disable-next-line no-console
  console.log('listening on:', port);
});
