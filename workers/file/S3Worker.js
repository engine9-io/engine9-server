const debug = require('debug')('S3Worker');
const fs = require('node:fs');
const { getTempFilename } = require('@engine9/packet-tools');
const { S3Client, GetObjectCommand, GetObjectAttributesCommand } = require('@aws-sdk/client-s3');

const BaseWorker = require('../BaseWorker');

function Worker(worker) {
  BaseWorker.call(this, worker);
}

require('util').inherits(Worker, BaseWorker);

Worker.metadata = {
  alias: 's3',
};

function getParts(filename) {
  if (!filename || filename.indexOf('s3://') !== 0) throw new Error(`Invalid filename for s3:${filename}`);
  const parts = filename.split('/');
  const Bucket = parts[2];
  const Key = parts.slice(3).join('/');
  return { Bucket, Key };
}
Worker.prototype.getClient = function () {
  if (!this.client) this.client = new S3Client({});
  return this.client;
};

Worker.prototype.getMetadata = async function ({ filename }) {
  const s3Client = this.getClient();
  const { Bucket, Key } = getParts(filename);

  const resp = await s3Client.send(new GetObjectAttributesCommand({
    Bucket,
    Key,
    ObjectAttributes: ['ETag', 'Checksum', 'ObjectParts', 'StorageClass', 'ObjectSize'],
  }));

  return resp;
};
Worker.prototype.getMetadata.metadata = {
  options: {
    filename: {},
  },
};

Worker.prototype.getStream = async function ({ filename }) {
  const s3Client = new S3Client({});
  const { Bucket, Key } = getParts(filename);
  const command = new GetObjectCommand({ Bucket, Key });
  const response = await s3Client.send(command);
  debug(`Streaming file ${Key}`);

  return response.Body;
};
Worker.prototype.getStream.metadata = {
  options: {
    filename: {},
  },
};

Worker.prototype.download = async function ({ filename }) {
  const file = filename.split('/').pop();
  const localPath = await getTempFilename({ targetFilename: file });
  const s3Client = new S3Client({});
  const { Bucket, Key } = getParts(filename);
  const command = new GetObjectCommand({ Bucket, Key });
  debug(`Downloading ${file} to ${localPath}`);
  const response = await s3Client.send(command);
  const fileStream = fs.createWriteStream(localPath);

  response.Body.pipe(fileStream);

  return new Promise((resolve, reject) => {
    fileStream.on('finish', async () => {
      const { size } = await fs.promises.stat(localPath);
      resolve({ size, filename: localPath });
    });
    fileStream.on('error', reject);
  });
};
Worker.prototype.download.metadata = {
  options: {
    filename: {},
  },
};

module.exports = Worker;
