const debug = require('debug')('S3Worker');
const fs = require('node:fs');
const { getTempFilename } = require('@engine9/packet-tools');
const {
  S3Client, GetObjectCommand, GetObjectAttributesCommand, PutObjectCommand,
} = require('@aws-sdk/client-s3');

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

Worker.prototype.stream = async function ({ filename }) {
  const s3Client = new S3Client({});
  const { Bucket, Key } = getParts(filename);
  const command = new GetObjectCommand({ Bucket, Key });
  const response = await s3Client.send(command);
  debug(`Streaming file ${Key}`);

  return { stream: response.Body };
};
Worker.prototype.stream.metadata = {
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

Worker.prototype.put = async function (options) {
  const { filename, directory } = options;
  if (!filename) throw new Error('Local filename required');
  if (directory?.indexOf('s3://') !== 0) throw new Error(`directory path must start with s3://, is ${directory}`);

  const file = options.file || filename.split('/').pop();
  const parts = directory.split('/');
  const Bucket = parts[2];
  const Key = parts.slice(3).filter(Boolean).concat(file).join('/');
  const Body = fs.createReadStream(filename);

  debug(`Putting ${filename} to ${JSON.stringify({ Bucket, Key })}}`);
  const s3Client = new S3Client({});

  const command = new PutObjectCommand({ Bucket, Key, Body });

  return s3Client.send(command);
};
Worker.prototype.put.metadata = {
  options: {
    filename: {},
    directory: { description: 'Directory to put file, e.g. s3://foo-bar/dir/xyz' },
    file: { description: 'Name of file, defaults to the filename' },
  },
};

Worker.prototype.write = async function (options) {
  const { directory, file, content } = options;

  if (!directory?.indexOf('s3://')) throw new Error('directory must start with s3://');
  const parts = directory.split('/');

  const Bucket = parts[2];
  const Key = parts.slice(3).filter(Boolean).concat(file).join('/');
  const Body = content;

  debug(`Writing content of length ${content.length} to ${JSON.stringify({ Bucket, Key })}}`);
  const s3Client = new S3Client({});

  const command = new PutObjectCommand({ Bucket, Key, Body });

  return s3Client.send(command);
};
Worker.prototype.write.metadata = {
  options: {
    directory: { description: 'Directory to put file, e.g. s3://foo-bar/dir/xyz' },
    file: { description: 'Name of file, defaults to the filename' },
    content: { description: 'Contents of file' },
  },
};

module.exports = Worker;
