const { google } = require('googleapis');
const fs = require('node:fs');

const fsp = fs.promises;

const BaseWorker = require('../BaseWorker');

function Worker(worker) {
  BaseWorker.call(this, worker);
}

require('util').inherits(Worker, BaseWorker);

Worker.metadata = {
  alias: 'gdrive',
};
Worker.prototype.setAuth = async function () {
  const keyFile = process.env.GOOGLE_APPLICATION_CREDENTIALS;
  const settings = JSON.parse(await fsp.readFile(keyFile));
  if (!settings.subject_to_impersonate) throw new Error(`You should include subject_to_impersonate in file ${keyFile}`);

  const auth = new google.auth.GoogleAuth({
    clientOptions: {
      subject: settings.subject_to_impersonate,
    },
    keyFile,
    scopes: ['https://www.googleapis.com/auth/drive'],
  });
  google.options({
    auth,
  });
};

Worker.prototype.list = async function ({ path }) {
  await this.setAuth();
  const drive = google.drive({ version: 'v3' });
  const folderId = path;
  const q = `'${folderId}' in parents and trashed=false`;
  const raw = await drive.files.list({
    pageSize: 150,
    q,
    supportsAllDrives: true, // include share drives as well
    includeItemsFromAllDrives: true,
  });

  return raw.data?.files;
};
Worker.prototype.list.metadata = {
  options: {
    path: {},
  },
};

module.exports = Worker;
