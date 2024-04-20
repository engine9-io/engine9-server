/* eslint-disable camelcase */
/*
  The PacketWorker is in charge of reading, pulling, and pushing packets from the
  core repository
*/

const util = require('node:util');
const fs = require('node:fs');
const debug = require('debug')('PacketWorker');
const AWS = require('aws-sdk');
const unzipper = require('unzipper');

const BaseWorker = require('./BaseWorker');

function Worker(worker) {
  BaseWorker.call(this, worker);
}

util.inherits(Worker, BaseWorker);

Worker.prototype.getClient = function () {
  return new AWS.S3({
    region: process.env.AWS_REGION,
  });
};

Worker.prototype.listOld = async function ({ packet_path }) {
  const zip = fs.createReadStream(packet_path).pipe(unzipper.Parse({ forceStream: true }));
  for await (const entry of zip) {
    const fileName = entry.path;
    const { type } = entry; // 'Directory' or 'File'
    const size = entry.vars.uncompressedSize; // There is also compressedSize;
    debugger;
    console.log('Found filename:', filename);
    if (fileName === "this IS the file I'm looking for") {
      entry.pipe(fs.createWriteStream('sampleoutput.txt'));
    } else {
      entry.autodrain();
    }
  }
};

Worker.prototype.list = async function ({ packet_path }) {
  const directory = await unzipper.Open.file(packet_path);
  console.log('directory', directory);
  // const files = await directory.files[0];
  console.log(directory.files);
  console.log(directory.files[0]);

  debugger;

  return new Promise((resolve, reject) => {
    directory.files[0]
      .stream()
      .pipe(fs.createWriteStream('firstFile'))
      .on('error', (e) => {
        reject;
      })
      .on('finish', resolve);
  });
};
Worker.prototype.list.metadata = {
  options: {},
};

// List contents of a remote packet
Worker.prototype.get = async function ({ packet_path }) {
  const s3Client = await this.getClient();
  const url = new URL(packet_path);
  if (url.protocol !== 's3:') throw new Error('Invalid protocol');

  const directory = await unzipper.Open.s3(s3Client, {
    Bucket: url.host, Key: url.pathname.slice(1),
  });

  const files = await directory.files;
  console.log(Object.keys(files[0]));

  return new Promise((resolve, reject) => {
    directory.files[0]
      .stream()
      .pipe(fs.createWriteStream('firstFile'))
      .on('error', reject)
      .on('finish', resolve);
  });

  // return directory.files.map((f) => f.path);

  /* return new Promise((  resolve, reject) => {
    directory.files[0]
      .stream()
      .pipe(fs.createWriteStream('firstFile'))
      .on('error', reject)
      .on('finish', resolve);
  });
  */
};
Worker.prototype.list.metadata = {
  options: {},
};

module.exports = Worker;
