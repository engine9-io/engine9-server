const express = require('express');
const bodyParser = require('body-parser');
const apiV1 = require('./api/v1');

const app = express();
const isDevelopment = process.env.NODE_ENV === 'development';

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));
if (isDevelopment) app.set('json spaces', 2); // number of spaces for indentation
app.use('/:account_id/api/v1', apiV1);
app.get('/ok', (req, res) => res.json({ ok: true, ...(isDevelopment ? { env: 'development' } : {}) }));

const server = app.listen(3001, () => {
  // eslint-disable-next-line no-console
  console.log('listening on port %s...', server.address().port);
});
