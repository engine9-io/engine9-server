const https = require('node:https');
const fs = require('node:fs');
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

const data = require('./object/data');

try {
  // eslint-disable-next-line global-require
  require('../account-config.json');
} catch (e) {
  throw new Error('Error loading account-config.json file -- make sure to create one from account-config.template.json before running');
}

const ui = require('./object/ui.console.config');
const { addUserToRequest } = require('./object/permissions');
const packetServer = require('./packet-server/index');

app.use(cors());
app.use(compression());
app.use(bodyParser.json());

app.get('/ok', (req, res) => { res.json({ ok: true }); });

app.use('/ui', ui);
app.use(addUserToRequest);

app.get('/user', (req, res) => { res.json({ user: req.user }); });

app.use('/data', data);

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

let port = 443; // default to ssl port

if (process.env.API_PORT) port = parseInt(process.env.API_PORT, 10);
if (process.env.SSL_CERT_PATH) {
  https.createServer({
    key: fs.readFileSync(`${process.env.SSL_CERT_PATH}/key.pem`),
    cert: fs.readFileSync(`${process.env.SSL_CERT_PATH}/cert.pem`),
  }, app).listen(port, (e) => {
    if (e) throw e;
    // eslint-disable-next-line no-console
    console.log('listening on:', port);
  });
} else {
  app.listen(port, (e) => {
    if (e) throw e;
    // eslint-disable-next-line no-console
    console.log('listening on:', port);
  });
}
