const express = require('express');

const app = express();
const v1 = require('./v1');

app.use('/api', v1);
app.get('/ok', (req, res) => {
  res.json({ ok: true });
});

const port = 80;

app.listen(port, (e) => {
  if (e) throw e;
  // eslint-disable-next-line no-console
  console.log('listening on:', port);
});
