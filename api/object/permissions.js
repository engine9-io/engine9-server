// A lot of libraries initialize using the process.env object, so keep this first
require('dotenv').config({ path: '../.env' });
const firebaseAdmin = require('firebase-admin');
const firebaseConfig = require('../../.firebase-credentials.json');
const accountConfig = require('../../account-config.json');

firebaseAdmin.initializeApp({
  credential: firebaseAdmin.credential.cert(firebaseConfig),
  databaseURL: firebaseConfig.databaseURL,
});

async function addUserToRequest(req, res, next) {
  try {
    const Authorization = req.get('Authorization');
    if (typeof Authorization !== 'string') {
      return res.status(401).json({ error: 'Missing Authorization header' });
    }
    const idToken = Authorization.replace(/BEARER /i, '');

    const user = await firebaseAdmin.auth().verifyIdToken(idToken);
    user.accounts = {};
    const id = user.uid;
    const isAdmin = (accountConfig.adminUserIds || []).find((i) => i === id);
    Object.entries(accountConfig.accounts).forEach(([accountId, o]) => {
      if (isAdmin) {
        user.accounts[accountId] = { name: o.name, level: 'admin' };
      } else if ((o.userIds || []).find((i) => i === id)) {
        user.accounts[accountId] = { name: o.name, level: 'normal' };
      }
    });
    req.user = user;
    return next();
  } catch (e) {
    // eslint-disable-next-line no-console
    console.error(e);
    return res.status(401).json({ error: 'Invalid authorization' });
  }
}

module.exports = { addUserToRequest };
