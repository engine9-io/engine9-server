// A lot of libraries initialize using the process.env object, so keep this first
require('dotenv').config({ path: '../.env' });
const firebaseAdmin = require('firebase-admin');
// eslint-disable-next-line global-require
const firebaseConfig = require('../../.firebase-credentials.json');

firebaseAdmin.initializeApp({
  credential: firebaseAdmin.credential.cert(firebaseConfig),
  databaseURL: firebaseConfig.databaseURL,
});

async function addUserToRequest(req, res, next) {
  const Authorization = req.get('Authorization');
  if (typeof Authorization !== 'string') {
    return res.status(401).json({ error: 'Missing Authorization header' });
  }
  const idToken = Authorization.replace(/BEARER /i, '');

  const user = await firebaseAdmin.auth().verifyIdToken(idToken);
  req.user = user;
  return next();
}

module.exports = { addUserToRequest };
