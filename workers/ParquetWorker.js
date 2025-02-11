const parquet = require('@dsnp/parquetjs');

const util = require('node:util');
const { Readable } = require('node:stream');
const debug = require('debug')('ParquetWorker');
const { S3Client } = require('@aws-sdk/client-s3');
const BaseWorker = require('./BaseWorker');
const FileWorker = require('./FileWorker');

function Worker(worker) {
  BaseWorker.call(this, worker);
}

util.inherits(Worker, BaseWorker);

Worker.metadata = {
  alias: 'file',
};

async function getReader(options) {
  const { path = 's3://engine9-accounts/test/big.csv.with_ids.parquet' } = options;
  if (path.indexOf('s3://') === 0) {
    const client = new S3Client({});
    const parts = path.split('/');

    return parquet.ParquetReader.openS3(client, {
      Bucket: parts[2],
      Key: parts.slice(3).join('/'),
    });
  }
  return parquet.ParquetReader.openFile(path);
}

Worker.prototype.meta = async function (options) {
  const reader = await getReader(options);
  return {
    records: String(reader.metadata?.num_rows),
  };
  // getMetadata();
};
Worker.prototype.meta.metadata = {
  options: {
    path: {},
  },
};
Worker.prototype.schema = async function (options) {
  const reader = await getReader(options);
  return reader.getSchema();
};
Worker.prototype.schema.metadata = {
  options: {
    path: {},
  },
};

Worker.prototype.stream = async function (options) {
  const stream = new Readable({ objectMode: true });

  const reader = await getReader(options);
  // create a new cursor
  const cursor = reader.getCursor(['id', 'person_id', 'ts']);

  // read all records from the file and print them
  let record = null;
  let counter = 0;
  const start = new Date().getTime();
  do {
    // eslint-disable-next-line no-await-in-loop
    record = await cursor.next();
    counter += 1;
    if (counter > 100) {
      stream.push(null);
      break;
    }
    if (counter % 5000 === 0) {
      const end = new Date().getTime();
      debug(`Read ${counter} ${(counter * 1000) / (end - start)}/sec `);
    }
    stream.push(record);
  } while (record);
  await reader.close();

  return { stream };
};

Worker.prototype.stream.metadata = {
  options: {
    path: {},
  },
};

Worker.prototype.toFile = async function (options) {
  const { stream } = await this.stream(options);
  const fworker = new FileWorker();
  return fworker.objectStreamToFile({ ...options, stream });
};
Worker.prototype.toFile.metadata = {
  options: {
    path: {},
  },
};

Worker.prototype.stats = async function (options) {
  const reader = await getReader(options);
  const schema = reader.getSchema();
  const fileMetadata = reader.getFileMetaData();
  const rowGroups = reader.getRowGroups();

  // const reader = await parquet.ParquetReader.openS3(client, getParams(options));
  // return reader.getSchema();
  return {
    schema,
    fileMetadata,
    rowGroups,
  };
};
Worker.prototype.stats.metadata = {
  options: {
    path: {},
  },
};

module.exports = Worker;
