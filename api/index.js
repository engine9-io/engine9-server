const express = require('express');

const app = express();
const cors = require('cors');
const compression = require('compression');
const bodyParser = require('body-parser');

const data = require('./v1/data');
const ui = require('./v1/ui');

app.use(cors());
app.use(compression());
app.use(bodyParser.json());

app.use('/data', data);
app.use('/ui', ui);
app.get('/ok', (req, res) => {
  res.json({ ok: true });
});

const port = 8080;

app.listen(port, (e) => {
  if (e) throw e;
  // eslint-disable-next-line no-console
  console.log('listening on:', port);
});
