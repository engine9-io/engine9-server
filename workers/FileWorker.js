const fs = require('node:fs');

const fsp = fs.promises;
const zlib = require('node:zlib');
const { promisify } = require('node:util');
const util = require('util');
const { Readable, Transform } = require('node:stream');

const { pipeline } = require('node:stream/promises');
const debug = require('debug')('FileWorker');
// const through2 = require('through2');
const csv = require('csv');
const es = require('event-stream');
const JSON5 = require('json5');// Useful for parsing extended JSON
const languageEncoding = require('detect-file-encoding-and-language');

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
  // Limit to only the top 1000 characters -- for perfomance
  const bytes = 10000;
  const buff = Buffer.alloc(bytes);
  const fd = await fsp.open(options.filename);
  await fd.read(buff, 0, bytes);
  let finalBuff = buff;
  if (options.filename.slice(-3) === '.gz') {
    finalBuff = await promisify(zlib.gunzip)(buff);
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
  const { filename } = options;
  if (!filename) throw new Error('fileToObjectStream: filename is required');
  let postfix = options.source_postfix || filename.toLowerCase().split('.').pop();
  if (postfix === 'zip') {
    debug('Invalid filename:', { filename });
    throw new Error('Cowardly refusing to turn a .zip file into an object stream, turn into a csv first');
  }

  const { encoding } = await this.detectEncoding(options);
  let count = 0;

  debug(`Reading file ${filename} with encoding:`, encoding);

  const head = null;
  let transforms = [];
  let stream = fs.createReadStream(filename);

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
  const { filename, stream: fileWriterStream } = await this.getFileWriterStream();
  let records = 0;
  await pipeline(
    inStream,
    new Transform({
      objectMode: true,
      transform(d, enc, cb) {
        records += 1;
        cb(null, d);
      },
    }),
    this.getJSONStringifyStream().stream,
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

  const targetFormat = options.target_format || options.targetFormat;

  if (!targetFormat && (filename.toLowerCase().slice(-4) === '.csv' || filename.toLowerCase().slice(-7) === '.csv.gz')) {
    options.target_format = 'csv';
  }

  return worker.objectStreamToFile({ ...options, stream });
};

Worker.prototype.transform.metadata = {
  options: {
    source_postfix: { description: "Override the source postfix, if for example it's a csv" },
    encoding: { description: 'Manual override of source file encoding' },
    names: { description: 'Target field names (e.g. my_new_field,x,y,z)' },
    values: { description: "Comma delimited source field name, or Handlebars [[ ]] merge fields (e.g. 'my_field,x,y,z', '[[field1]]-[[field2]]', etc)" },
    keep_source: { description: 'Keep original fields? (default false)' },
    field_delimiter: { description: 'Custom field delimiter for the file, if not specified uses a comma if present, else a tab' },
    target_filename: { description: 'Custom name of the output file (default auto-generated)' },
    target_format: { description: 'Output format -- csv supported, or none for txt (default)' },
    targetRowDelimiter: { description: 'Row delimiter (default \n)' },
    targetFieldDelimiter: { description: 'Field delimiter (default \t or ,)' },
    clean_header: { description: 'Clean the names in the incoming file, standardizing to [a-z0-9._] format' },
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

/* Get a stream from an actual stream, or an array, or a file */
Worker.prototype.getStream = async function ({ stream, filename } = {}) {
  if (stream) {
    if (typeof stream === 'object') return stream;
    if (Array.isArray(stream)) {
      return Readable.from(stream);
    }
    throw new Error(`Invalid stream type:${typeof stream}`);
  }

  return this.fileToObjectStream({ filename });
};

module.exports = Worker;
