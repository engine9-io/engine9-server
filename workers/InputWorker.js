/* eslint-disable no-restricted-syntax */
/* eslint-disable no-await-in-loop */
/* eslint-disable camelcase */

const util = require('node:util');
const { pipeline } = require('node:stream/promises');
const { Transform } = require('node:stream');
const fs = require('node:fs');

const fsp = fs.promises;
const path = require('node:path');

const zlib = require('node:zlib');
const parquet = require('@dsnp/parquetjs');
const csv = require('csv');
const { mkdirp } = require('mkdirp');
const debug = require('debug')('InputWorker');
const { uuidRegex, getTempFilename, TIMELINE_ENTRY_TYPES } = require('@engine9/packet-tools');
// const SQLWorker = require('./SQLWorker');
const SQLiteWorker = require('./sql/SQLiteWorker');
const PluginBaseWorker = require('./PluginBaseWorker');
const FileWorker = require('./FileWorker');
const S3Worker = require('./file/S3Worker');
const PersonWorker = require('./PersonWorker');
const { analyzeTypeToParquet, bool } = require('../utilities');

function Worker(worker) {
  PluginBaseWorker.call(this, worker);
  /*
    Input files can be stored in a directory, or s3
  */
  this.store_path = process.env.ENGINE9_STORED_INPUT_PATH;
  if (!this.store_path) throw new Error('No ENGINE9_STORED_INPUT_PATH configured');
}

util.inherits(Worker, PluginBaseWorker);

Worker.metadata = {
  alias: 'input',
};

/*
  Sometimes we like to use a SQLite DB for calculating stats
*/
Worker.prototype.getTimelineInputSQLiteDB = async function ({
  sqliteFile,
  inputId,
  includeEmailDomain = false,
}) {
  if (!sqliteFile && !inputId) throw new Error('getTimelineInputSQLiteDB requires a sqliteFile or inputId');
  let dbpath = sqliteFile;
  this.timelineSQLiteDBCache = this.timelineSQLiteDBCache || {};
  if (!dbpath) {
    const dir = [this.store_path, this.accountId, inputId.slice(0, 4), inputId].join(path.sep);
    await (mkdirp(dir));
    dbpath = `${dir + path.sep}input.db`;
  }

  let output = this.timelineSQLiteDBCache[dbpath];
  if (output) return output;
  const exists = await fsp.stat(dbpath).then(() => true).catch(() => false);
  const sqliteWorker = new SQLiteWorker({ sqliteFile: dbpath, accountId: this.accountId });
  await sqliteWorker.ensureTimelineSchema({ includeEmailDomain });

  if (exists) {
    output = { sqliteFile: dbpath, sqliteWorker };
    this.timelineSQLiteDBCache[dbpath] = output;
    return output;
  }

  output = { sqliteFile: dbpath, sqliteWorker };
  this.timelineSQLiteDBCache[dbpath] = output;
  return output;
};

Worker.prototype.destroy = async function () {
  Object.entries(this.timelineSQLiteDBCache || {}).forEach(([, { sqliteWorker }]) => {
    sqliteWorker.destroy();
  });
};

Worker.prototype.id = async function (options) {
  const worker = this;

  const {
    inputId, defaultEntryType, filename,
  } = options;
  let { pluginId } = options;
  if (!inputId) {
    throw new Error('id requires an inputId');
  }
  if (!pluginId) {
    const { data: inputData } = await this.query({ sql: 'select * from input where id=?', values: [inputId] });
    const input = inputData[0];
    if (!input) {
      throw new Error(`pluginId is required to identify new people, was not specified, and could not find input id ${inputId} in the database to track it down`);
    }
    pluginId = input.plugin_id;
  }

  const processId = `.${new Date().getTime()}.processing`;

  const fileWorker = new FileWorker(this);
  const personWorker = new PersonWorker(this);
  const { fields } = await fileWorker.analyze({ filename });

  const initialSchema = {
    // So, this is a UUID, but it's a pain to read them in the files, so do string instead
    // until node.js supports the UUID type natively
    // id: { parquetType: 'BYTE_ARRAY',parquetMap: (v) => v  },
    id: { parquetType: 'UTF8', parquetMap: (v) => v },
    ts: { parquetType: 'TIMESTAMP_MILLIS', parquetMap: (v) => new Date(v) },
    entry_type_id: { parquetType: 'INT32', parquetMap: (v) => parseInt(v, 10) },
    person_id: { parquetType: 'INT64', parquetMap: (v) => parseInt(v, 10) },
    source_code_id: { parquetType: 'INT64', parquetMap: (v) => parseInt(v, 10) },
  };
  if (fields.find((d) => d.name === 'email')) {
    initialSchema.email_domain = { parquetType: 'UTF8', parquetMap: (v, obj) => (obj.email || '').split('@').slice(-1)[0].toLowerCase() };
  }

  const fieldMap = fields.reduce((a, b) => {
    const pq = analyzeTypeToParquet(b.type);
    if (a[b.name]) return a;
    // ignore these, as they're specified by id, but keep the person info just in case
    if (['identifiers', 'entry_type', 'source_code'].indexOf(b.name) >= 0) return a;
    a[b.name] = {
      type: b.type,
      parquetType: pq.type,
      parquetMap: pq.map,
    };
    return a;
  }, initialSchema);

  const parquetSchemaDefinition = {};
  Object.entries(fieldMap).forEach(([name, obj]) => {
    parquetSchemaDefinition[name] = { type: obj.parquetType };
  });

  const parquetSchema = new parquet.ParquetSchema(parquetSchemaDefinition);

  let outputFile;
  if (filename.indexOf('/') === 0) {
    outputFile = `${filename}.id.parquet`;
  } else {
    outputFile = await getTempFilename({ postfix: '.id.parquet' });
  }

  const writer = await parquet.ParquetWriter.openFile(parquetSchema, `${outputFile}${processId}`, {
    bloomFilters: [
      {
        column: 'ts',
        numFilterBytes: 1024,
      },
    ],
  });

  const batcher = this.getBatchTransform({ batchSize: 500 }).transform;
  let records = 0;
  let batches = 0;
  await pipeline(
    (await fileWorker.fileToObjectStream({ filename })).stream,
    batcher,
    new Transform({
      objectMode: true,
      async transform(batch, encoding, cb) {
        // eslint-disable-next-line no-underscore-dangle
        if (batch[0]?._is_placeholder) {
          return cb(null);
        }
        /*
        //Don't check against the database, trust the input id to be right, even
        //with conflicting remote_input_ids -- not the responsibility here
        const invalidRemoteIds = batch
          .reduce((a, b) => {
            if (b.remote_input_id && b.remote_input_id !== input.remote_input_id) {
              a[b.remote_input_id] = (a[b.remote_input_id] || 0) + 1;
            }
            return a;
          }, {});
        if (Object.keys(invalidRemoteIds).length > 0) {
          debug(`Invalid file:${filename}`, 'Invalid remote_input_id=', invalidRemoteIds);
          throw new Error(`Error id'ing input ${inputId}:
            remote_input_id was specified in the incoming file
            (e.g. '${Object.keys(invalidRemoteIds)[0]}'),
             but the values do not match the expected
             input.remote_input_id=${input.remote_input_id}`);
        }
          */

        batches += 1;
        records += batch.length;
        if (batches % 10 === 0) debug(`Processed ${batches} batches, ${records} records`);
        worker.markPerformance('start-entry-type-id');
        await worker.appendEntryTypeId({ batch, defaultEntryType });
        worker.markPerformance('start-source-code-id');
        await worker.appendSourceCodeId({ batch });
        worker.markPerformance('start-upsert-person');
        await personWorker.loadPeople({ stream: batch, pluginId, inputId });
        worker.markPerformance('start-append-entry');
        await worker.appendEntryId({ inputId, batch });
        worker.markPerformance('end-batch');
        // parquetjs requires the appendRow to be completed before subsequent calls
        // eslint-disable-next-line no-restricted-syntax
        for (const b of batch) {
          const row = {};
          Object.entries(fieldMap).forEach(([name, { parquetMap }]) => {
            row[name] = parquetMap(b[name], b);
          });
          await writer.appendRow(row);
        }
        debug(`Wrote ${records} records`);

        return cb();
      },
    }),
  );
  await writer.close();
  debug(`Finished writing ${outputFile}`);

  await fsp.rename(`${outputFile}${processId}`, `${outputFile}`);
  // copy it back to s3
  if (filename.indexOf('s3://') === 0) {
    // debug(`Copying ${outputFile} to s3`);
    const s3 = new S3Worker(this);

    const directory = filename.split('/').slice(0, -1).join('/');
    const s3Results = await s3.put({ filename: outputFile, directory });
    const file = outputFile.split('/').pop();
    return {
      records, idFilename: `${directory}/${file}`, sourceIdFilename: outputFile, s3Results,
    };
  }
  return { records, idFilename: outputFile, fields };
};

Worker.prototype.id.metadata = {
  options: {
    filename: {},
    defaultEntryType: {
      description: 'Default entry type if not specified in the file',
    },
  },
};

/*
  Add ids to an input file -- presumed to already be split by inputs
*/
Worker.prototype.idCSV = async function (options) {
  const worker = this;

  const {
    inputId, defaultEntryType, filename,
  } = options;
  if (!inputId) throw new Error('id requires an inputId');
  const processId = `.${new Date().getTime()}.processing`;

  const fileWorker = new FileWorker(this);
  const personWorker = new PersonWorker(this);

  const idStream = csv.stringify({ header: true });
  let outputFile;
  if (filename.indexOf('/') === 0) {
    outputFile = `${filename}.id.csv.gz`;
  } else {
    outputFile = await getTempFilename({ postfix: '.id.csv.gz' });
  }

  const idFileStream = fs.createWriteStream(`${outputFile}${processId}`);

  idStream
    .pipe(zlib.createGzip())
    .pipe(idFileStream);

  const batcher = this.getBatchTransform({ batchSize: 500 }).transform;
  let records = 0;
  let batches = 0;
  await pipeline(
    (await fileWorker.fileToObjectStream({ filename })).stream,
    batcher,
    new Transform({
      objectMode: true,
      async transform(batch, encoding, cb) {
        // eslint-disable-next-line no-underscore-dangle
        if (batch[0]?._is_placeholder) {
          return cb(null);
        }
        const initialItem = { ...batch[0] };

        batches += 1;
        records += batch.length;
        if (batches % 10 === 0) debug(`Processed ${batches} batches, ${records} records`);
        worker.markPerformance('start-entry-type-id');
        await worker.appendEntryTypeId({ batch, defaultEntryType });
        worker.markPerformance('start-source-code-id');
        await worker.appendSourceCodeId({ batch });
        worker.markPerformance('start-upsert-person');
        await personWorker.appendPersonId({ batch, inputId });
        worker.markPerformance('start-append-entry');
        await worker.appendEntryId({ inputId, batch });
        worker.markPerformance('end-batch');
        batch.forEach((b) => {
          const {
            id, ts, entry_type_id, person_id, source_code_id,
          } = b;
          delete b.identifiers;
          delete b.source_input_id;// we already know these
          delete b.input_id;
          // eslint-disable-next-line prefer-destructuring
          if (b.email) b.email_domain = b.email.split('@').slice(-1)[0].toLowerCase();
          if (initialItem.email_hash_v1 === undefined) delete b.email_hash_v1;
          if (initialItem.phone_hash_v1 === undefined) delete b.phone_hash_v1;

          const withId = {
            id, ts, entry_type_id, person_id, source_code_id, ...b,
          };
          idStream.write(withId);
        });

        return cb();
      },
    }),
  );
  await idStream.end();

  await new Promise((resolve) => { idFileStream.on('finish', resolve); });

  await fsp.rename(`${outputFile}${processId}`, `${outputFile}`);

  return { records, idFilename: `${outputFile}` };
};

Worker.prototype.idCSV.metadata = {
  options: {
    filename: {},
    defaultEntryType: {
      description: 'Default entry type if not specified in the file',
    },
  },
};

/*
//Sample multi-file inputs
fileArray: [
    {
      inputId: '2b335df4-940d-52c9-94d2-b3a06883bc1b',
      remoteInputId: 'abc1230',
      source_filename:
      '/var/folders/59/2025-02-09/2025_02_09_20_01_07_921_3497.input.csv.gz',
      filename: 's3://engine9-accounts/dev/stored_input/2b33/2b335df4-940d-52c9-94d2-b3a06883bc1b/2025_02_09_20_01_07_921_3497.input.csv.gz',
      records: 4
    },
    {
      inputId: '16f8d68e-8c37-51b3-8c87-dc67ac305d69',
      remoteInputId: 'abc1232',
      source_filename: '/var/folders/59/T/dev/2025-02-09/2025_02_09_20_01_07_924_9112.input.csv.gz',
      filename: 's3://engine9-accounts/dev/stored_input/16f8/16f8d68e-8c37-51b3-8c87-dc67ac305d69/2025_02_09_20_01_07_924_9112.input.csv.gz',
      records: 3
    }
  ],

*/
Worker.prototype.idAndLoadFiles = async function (options) {
  const loadTimeline = bool(options.loadTimeline, false);
  if (options.detailsTable && !loadTimeline) throw new Error('Cowardly refusing to load details table without loadTimeline as well');
  let arr = options.fileArray;
  if (typeof arr === 'string') arr = JSON.parse(arr);
  if (!Array.isArray(arr))arr = [options];
  const output = [];
  const directories = {};
  // eslint-disable-next-line no-restricted-syntax
  for (const o of arr) {
    const { inputId, pluginId, filename } = o;
    const { idFilename } = await this.id({ filename, inputId, pluginId });
    // debug(`After id, idFilename=${idFilename}`);

    let timelineResults = null;
    if (loadTimeline) {
      timelineResults = await this.loadTimeline({
        filename: idFilename,
        inputId,
      });
    }
    let detailResults = null;
    if (options.detailsTable) {
      detailResults = await this.loadTimelineDetails({ filename, inputId });
    }
    directories[idFilename.split('/').slice(0, -1).join('/')] = true;
    const outputVals = {
      inputId,
      idFilename,
      sourceFilename: filename,
    };
    if (timelineResults) outputVals.timelineResults = timelineResults;
    if (detailResults) outputVals.detailResults = detailResults;

    output.push(outputVals);
  }

  return {
    fileArray: output,
    directoryArray: Object.keys(directories).map((directory) => ({ directory })),
  };
};
Worker.prototype.idAndLoadFiles.metadata = {
  options: {
    fileArray: {},
    filename: {},
    inputId: {},
    loadTimeline: { description: 'Whether to load the timeline table or not, default false' },
    detailsTable: { description: 'Load this details table' },
  },
};

/*
  Calculate statistics for a directory using an in-memory
  SQLite database for deduplication.
  Requires an idFilename (so just a random filename shouldn't be run)
*/

Worker.prototype.statistics = async function (options) {
  const writeStatisticsFile = bool(options.writeStatisticsFile, false);
  const fileWorker = new FileWorker(this);
  let arr = options.directoryArray;
  if (typeof arr === 'string') {
    if (arr.indexOf('[') === 0) arr = JSON.parse(arr);
    else arr = arr.split(',').map((directory) => ({ directory }));
  }
  if (!Array.isArray(arr))arr = [options];
  const results = [];
  for (const d of arr) {
    let files = [];

    if (d.idFilename) {
      files.push(d);
    } else if (d.directory) {
      files = (await fsp.readdir(d.directory))
        .filter((f) => f.endsWith('.id.parquet'))
        .map((f) => ({ idFilename: `${d.directory}/${f}` }));
    } else {
      throw new Error(`Invalid directory for statistics:${JSON.stringify(d)}`);
    }
    if (files.length === 0) {
      results.push({
        no_files: true,
        ...d,
      });
      // eslint-disable-next-line no-continue
      continue;
    }

    const sqliteWorker = new SQLiteWorker({ sqliteFile: ':memory:', accountId: this.accountId });
    sqliteWorker.connect();

    const sources = [];
    let includeEmailDomain;
    // eslint-disable-next-line no-restricted-syntax
    for (const o of files) {
      const { idFilename } = o;
      const { columns, sqliteFile, ...timeline } = await sqliteWorker.loadTimeline({
        filename: idFilename,
        includeEmailDomain,
      });
      if (includeEmailDomain === undefined) {
        if (columns.find((c) => c.name === 'email_domain')) {
          includeEmailDomain = true;
        } else {
          includeEmailDomain = false;
        }
      }
      sources.push({ ...timeline, idFilename });
    }
    const { data } = await sqliteWorker.query(`select count(*) as records,
      count(distinct person_id) as distinct_people,
      min(ts) as min_ts,
      max(ts) as max_ts from timeline`);
    const statistics = data[0];
    statistics.min_ts = new Date(data[0].min_ts).toISOString();
    statistics.max_ts = new Date(data[0].max_ts).toISOString();

    statistics.byType = (await sqliteWorker.query(`
        select entry_type_id,
        count(*) as records,
        count(distinct person_id) as distinct_people
        from timeline group by entry_type_id`)).data?.map(({ entry_type_id, ...rest }) => ({
      entry_type: TIMELINE_ENTRY_TYPES[entry_type_id],
      ...rest,
    }));
    statistics.byTypeByHour = (await sqliteWorker.query(`
        select strftime('%Y-%m-%d %H:00:00', ts/1000, 'unixepoch') as date,
        entry_type_id,
        count(*) as records,count(distinct person_id) as distinct_people 
        from timeline group by 1,2 order by 1,2`)).data?.map(({ entry_type_id, ...rest }) => ({
      entry_type: TIMELINE_ENTRY_TYPES[entry_type_id],
      ...rest,
    }));

    if (includeEmailDomain) {
      const { data: domains } = await sqliteWorker.query('select email_domain,count(*) as records from timeline group by 1 order by count(*) desc limit 50');

      statistics.byEmailDomain = (await sqliteWorker.query({
        sql: `select email_domain,
        entry_type_id,
        count(*) as records,
        count(distinct person_id) as distinct_people
        from timeline 
        where email_domain in (${domains.map(() => '?').join(',')})
        group by email_domain,entry_type_id`,
        values: domains.map((o) => o.email_domain),
      })).data?.map(({ entry_type_id, ...rest }) => ({
        entry_type: TIMELINE_ENTRY_TYPES[entry_type_id],
        ...rest,
      }));
    }
    await sqliteWorker.destroy();
    const result = {
      sources,
      statistics,
    };
    if (d.directory) {
      result.directory = d.directory;
      if (writeStatisticsFile) {
        result.statisticsFile = `${d.directory}/statistics.json`;
        await fileWorker.write({
          filename: result.statisticsFile,
          content: JSON.stringify({ sources, statistics }, null, 4),
        });
      }
    } else if (d.idFilename) {
      result.idFilename = d.idFilename;
      if (writeStatisticsFile) {
        result.statisticsFile = `${d.idFilename}.statistics.json`;
        await fileWorker.write({
          filename: result.statisticsFile,
          content: JSON.stringify({ sources, statistics }, null, 4),
        });
      }
    }
    results.push(result);
  }

  return results;
};

Worker.prototype.statistics.metadata = {
  options: {
    /*
      Provide an array of directories, or a directory, or an actual filename
    */
    directoryArray: {},
    directory: {},
    idFilename: {},
    writeStatisticsFile: {
      description: 'Write the statistics file back to the directory,default false',
    },
  },
};

/*
  Calculate statistics for a whole directory, and put back a statistics file
*/
Worker.prototype.calculateAndPutStatisticsFile = async function (options) {
  return { ...options };
};

Worker.prototype.calculateAndPutStatisticsFile.metadata = {
  options: {
    directory: {},
  },
};

Worker.prototype.loadTimeline = async function (options) {
  if (!options.inputId) throw new Error('loadTimeline requires an inputId');
  const fileWorker = new FileWorker(this);
  const columns = ['id', 'ts', 'person_id', 'entry_type_id', 'source_code_id'];
  const { stream: inputStream } = await fileWorker.stream({ ...options, columns });

  const stream = inputStream.pipe(
    new Transform({
      objectMode: true,
      async transform(item, enc, cb) {
        if (!uuidRegex.test(item.id)) {
          throw new Error('loadTimeline requires items to have a uuid style id');
        }
        const missing = columns.filter((k) => item[k] === undefined);
        if (missing.length > 0) throw new Error(`loadTimeline is missing required fields ${missing.join(',')}`);
        cb(null, item);
      },
    }),
  );

  return this.insertFromStream({
    table: 'timeline', stream, upsert: true, defaults: { input_id: options.inputId },
  });
};
Worker.prototype.loadTimeline.metadata = {
  options: {
    stream: {},
    filename: {},
  },
};

Worker.prototype.loadTimelineDetails = async function (options) {
  const { columns } = await this.describe(options);

  const columnNames = columns.map((d) => d.name);

  const fileWorker = new FileWorker(this);
  const { stream } = await fileWorker.stream({ ...options, columns: columnNames });
  return this.insertFromStream({ table: options.table, stream, upsert: true });
};
Worker.prototype.loadTimelineDetails.metadata = {
  options: {
    stream: {},
    filename: {},
    columns: {},
  },
};

module.exports = Worker;
