const fs = require('node:fs');

const fsp = fs.promises;
const zlib = require('node:zlib');
const util = require('node:util');
const { Readable, Transform, PassThrough } = require('node:stream');

const { pipeline } = require('node:stream/promises');
const { stringify } = require('csv');
const PacketTools = require('@engine9/packet-tools');
const debug = require('debug')('FileWorker');
// const through2 = require('through2');
const csv = require('csv');
const es = require('event-stream');
const JSON5 = require('json5');// Useful for parsing extended JSON
const languageEncoding = require('detect-file-encoding-and-language');
const S3Worker = require('./file/S3Worker');
const ParquetWorker = require('./ParquetWorker');
const analyzeStream = require('../utilities/analyze');

const { bool } = require('../utilities');
const BaseWorker = require('./BaseWorker');

function Worker(worker) {
  BaseWorker.call(this, worker);
}

util.inherits(Worker, BaseWorker);

Worker.metadata = {
  alias: 'file',
};

Worker.prototype.csvToObjectTransforms = function (options) {
  const transforms = [];

  const headerMapping = options.headerMapping || function (d) { return d; };
  let lastLine = null;
  let head = null;

  const skipLinesWithError = bool(options.skip_lines_with_error, false);
  const parserOptions = {
    relax: true, skip_empty_lines: true, delimiter: ',', max_limit_on_data_read: 10000000, skip_lines_with_error: skipLinesWithError,
  };
  if (options.skip) parserOptions.from_line = options.skip;
  if (options.relax_column_count) parserOptions.relax_column_count = true;
  if (options.quote_escape) {
    parserOptions.escape = options.quote_escape;
  }

  debug('Parser options=', parserOptions);
  const parser = csv.parse(parserOptions);
  parser.on('error', (error) => {
    debug('fileToObjectStream: Error parsing csv file');
    debug(lastLine);
    throw new Error(error);
  });

  const blankAndHeaderCheck = new Transform({
    objectMode: true,
    transform(row, enc, cb) {
      // Blank rows
      if (row.length === 0) return cb();
      if (row.length === 1 && !row[0]) return cb();

      if (!head) {
        head = row.map(headerMapping);
        return cb();
      }

      const o = {};
      head.forEach((_h, i) => {
        const h = _h.trim();
        if (h) {
          o[h] = row[i];
        }
      });

      lastLine = row.join(',');
      return cb(null, o);
    },
  });

  transforms.push(parser);
  transforms.push(blankAndHeaderCheck);

  return { transforms };
};

Worker.prototype.detectEncoding = async function (options) {
  if (options.encoding_override) return { encoding: options.encoding_override };
  // Limit to only the top N bytes -- for perfomance
  // Be wary, though, as gzip files may require a certain minimum number of bytes to decompress
  const bytes = 64 * 1024;
  const buff = Buffer.alloc(bytes);
  const fd = await fsp.open(options.filename);
  await fd.read(buff, 0, bytes);
  let finalBuff = buff;
  if (options.filename.slice(-3) === '.gz') {
    // This code deals with scenarios where the buffer coming in may not be exactly the gzip
    // needed chunk size.
    finalBuff = await new Promise((resolve, reject) => {
      const bufferBuilder = [];
      const decompressStream = zlib.createGunzip()
        .on('data', (chunk) => {
          bufferBuilder.push(chunk);
        }).on('close', () => {
          resolve(Buffer.concat(bufferBuilder));
        }).on('error', (err) => {
          if (err.errno !== -5) {
            // EOF: expected
            reject(err);
          }
        });
      decompressStream.write(buff);
      decompressStream.end();
    });
  }

  return languageEncoding(finalBuff);
};

Worker.prototype.detectEncoding.metadata = {
  options: {
    filename: { required: true },
  },
};

/*
Internal method to transform a file into a stream of objects.
*/
Worker.prototype.fileToObjectStream = async function (options) {
  const { filename, columns, limit: limitOption } = options;
  let limit;
  if (limitOption) limit = parseInt(limitOption, 10);
  if (!filename) throw new Error('fileToObjectStream: filename is required');
  let postfix = options.sourcePostfix || filename.toLowerCase().split('.').pop();
  if (postfix === 'zip') {
    debug('Invalid filename:', { filename });
    throw new Error('Cowardly refusing to turn a .zip file into an object stream, turn into a csv first');
  }
  let encoding; let stream;
  if (filename.slice(-8) === '.parquet') {
    const pq = new ParquetWorker(this);
    return pq.stream({ filename, columns, limit });
  } if (filename.indexOf('s3://') === 0) {
    const s3Worker = new S3Worker(this);
    stream = (await s3Worker.stream({ filename, columns, limit })).stream;
    encoding = 'UTF-8';
  } else {
    stream = fs.createReadStream(filename);
    encoding = (await this.detectEncoding(options)).encoding;
  }

  let count = 0;

  debug(`Reading file ${filename} with encoding:`, encoding);

  const head = null;
  let transforms = [];

  if (postfix === 'gz') {
    const gunzip = zlib.createGunzip();
    transforms.push(gunzip);
    gunzip.setEncoding(encoding);
    // encoding = null;// Default encoding
    postfix = filename.toLowerCase().split('.');
    postfix = postfix[postfix.length - 2];
    debug(`Using gunzip parser because postfix is .gz, encoding=${encoding}`);
  } else {
    stream.setEncoding(encoding);
  }

  if (postfix === 'csv') {
    const csvTransforms = this.csvToObjectTransforms({ ...options });
    transforms = transforms.concat(csvTransforms.transforms);
  } else if (postfix === 'jsonl') {
    /* Type of JSON that has the names in an array in the first record,
    and the values in JSON arrays thereafter
    */
    let headers = null;

    const jsonlTransform = new Transform({
      objectMode: true,
      transform(d, enc, cb) {
        if (!d) return cb();
        const obj = JSON5.parse(d);
        /* JSONL could potentially start with an array of names,
        in which case we need to map the subsequent values
      */
        if (headers === null) {
          if (Array.isArray(obj)) {
            headers = obj;
            return cb();
          }
          headers = false;
        }
        if (headers) {
          const mapped = {};
          headers.forEach((name, i) => { mapped[name] = obj[i]; });
          this.push(mapped);
        } else {
          this.push(obj);
        }
        return cb();
      },
    });

    transforms.push(es.split());
    transforms.push(jsonlTransform);
  } else {
    throw new Error(`Unsupported file type: ${postfix}`);
  }
  const countAndDebug = new Transform({
    objectMode: true,
    transform(d, enc, cb) {
      if (count === 0) { debug('Sample object from file:', d); }
      count += 1;
      if ((count < 5000 && count % 1000 === 0) || (count % 50000 === 0)) {
        debug(`fileToObjectStream transformed ${count} lines`);
      }
      this.push(d);
      cb();
    },
    flush(cb) {
      // If there's no records at all, push a dummy record, and specify 0 records
      debug(`Completed reading file, records=${count}`);
      if (count === 0) {
        const o = { _is_placeholder: true };

        if (head) head.forEach((c) => { o[c] = null; });
        this.push(o);
      }
      cb();
    },
  });

  transforms.push(countAndDebug);
  transforms.forEach((t) => {
    stream = stream.pipe(t);
  });

  return { stream };
};
Worker.prototype.objectStreamToFile = async function (options) {
  const { stream: inStream } = options;
  const { filename, stream: fileWriterStream } = await this.getFileWriterStream(options);
  let records = 0;
  let stringifier;
  if (options.targetFormat === 'jsonl') {
    stringifier = this.getJSONStringifyTransform().transform;
  } else {
    stringifier = stringify({ header: true });
  }
  let gzip = new PassThrough();
  if (options.gzip) {
    gzip = zlib.createGzip();
  }
  await pipeline(
    inStream,
    new Transform({
      objectMode: true,
      transform(d, enc, cb) {
        records += 1;
        cb(null, d);
      },
    }),
    stringifier,
    gzip,
    fileWriterStream,
  );
  return { filename, records };
};

Worker.prototype.transform = async function (options) {
  const worker = this;

  const filename = worker.getFilename(options);

  debug(`Transforming ${filename}`);

  options.filename = filename;
  let { stream } = await worker.fileToObjectStream(options);
  if (typeof stream.pipe !== 'function') {
    debug(stream);
    throw new Error('No pipe in stream');
  }

  let t = options.transform;

  // No longer need this
  delete options.transform;
  if (!t) {
    t = function (d, enc, cb) {
      d.is_test_transform = true;
      cb(null, d);
    };
  }

  if (!Array.isArray(t)) t = [t];
  Object.keys(t).forEach((key) => {
    let f = t[key];
    if (typeof f === 'function') {
      f = new Transform({
        objectMode: true,
        transform: f,
      });
    }

    stream = stream.pipe(f);
  });

  const { targetFormat } = options;

  if (!targetFormat && (filename.toLowerCase().slice(-4) === '.csv' || filename.toLowerCase().slice(-7) === '.csv.gz')) {
    options.targetFormat = 'csv';
  }

  return worker.objectStreamToFile({ ...options, stream });
};

Worker.prototype.transform.metadata = {
  options: {
    sourcePostfix: { description: "Override the source postfix, if for example it's a csv" },
    encoding: { description: 'Manual override of source file encoding' },
    names: { description: 'Target field names (e.g. my_new_field,x,y,z)' },
    values: { description: "Comma delimited source field name, or Handlebars [[ ]] merge fields (e.g. 'my_field,x,y,z', '[[field1]]-[[field2]]', etc)" },
    targetFilename: { description: 'Custom name of the output file (default auto-generated)' },
    targetFormat: { description: 'Output format -- csv supported, or none for txt (default)' },
    targetRowDelimiter: { description: 'Row delimiter (default \n)' },
    targetFieldDelimiter: { description: 'Field delimiter (default \t or ,)' },
  },
};
Worker.prototype.testTransform = async function (options) {
  return this.transform({
    ...options,
    transform(d, enc, cb) { d.transform_time = new Date(); cb(null, d); },
  });
};
Worker.prototype.testTransform.metadata = {
  options: {
    filename: true,
  },
};

/* Get a stream from an actual stream, or an array, or a file, or a packet */
Worker.prototype.stream = async function ({
  stream, filename, packet, type, columns, limit,
} = {}) {
  if (stream) {
    if (Array.isArray(stream)) {
      return { stream: Readable.from(stream) };
    }
    // probably already a stream
    if (typeof stream === 'object') return { stream };
    throw new Error(`Invalid stream type:${typeof stream}`);
  } else if (filename) {
    return this.fileToObjectStream({ filename, columns, limit });
  } else if (packet) {
    let { stream: packetStream } = await PacketTools.stream({ packet, type, limit });
    const { transforms } = this.csvToObjectTransforms({});
    transforms.forEach((t) => {
      packetStream = packetStream.pipe(t);
    });
    return { stream: packetStream };
  } else {
    throw new Error('stream must be passed a stream, filename, or packet');
  }
};

Worker.prototype.analyze = async function (opts) {
  const { stream } = await this.stream(opts);
  return analyzeStream({ stream });
};
Worker.prototype.analyze.metadata = {
  options: {
    filename: {},

  },
};

Worker.prototype.write = async function (opts) {
  const { filename, content } = opts;
  if (filename.indexOf('s3://') === 0) {
    const s3Worker = new S3Worker(this);
    const parts = filename.split('/');
    const directory = parts.slice(0, -1).join('/');
    const file = parts.slice(-1)[0];
    debug(JSON.stringify({ parts, directory, file }));
    await s3Worker.write({
      directory,
      file,
      content,
    });
  } else {
    await fsp.writeFile(filename, content);
  }
  return { success: true, filename };
};
Worker.prototype.write.metadata = {
  options: {
    filename: { description: 'Location to write content to, can be local or s3://' },
    content: {},
  },
};

Worker.prototype.list = async function ({ directory }) {
  if (!directory) throw new Error('directory is required');
  if (directory.indexOf('s3://') === 0) {
    const s3Worker = new S3Worker(this);
    return s3Worker.list({ directory });
  }
  return fsp.readdir(directory);
};
Worker.prototype.list.metadata = {
  options: {
    directory: { required: true },
  },
};

module.exports = Worker;
