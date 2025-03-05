/* eslint-disable no-restricted-syntax */
/* eslint-disable no-await-in-loop */
/* eslint-disable camelcase */

const util = require('node:util');
const { pipeline } = require('node:stream/promises');
const { Transform } = require('node:stream');
const StreamConsumers = require('node:stream/consumers');
const fs = require('node:fs');

const fsp = fs.promises;
const path = require('node:path');

const zlib = require('node:zlib');
const parquet = require('@dsnp/parquetjs');
const csv = require('csv');
const { mkdirp } = require('mkdirp');
const debug = require('debug')('InputWorker');
const { uuidIsValid, getTempFilename, TIMELINE_ENTRY_TYPES } = require('@engine9/packet-tools');
// const SQLWorker = require('./SQLWorker');
const SQLiteWorker = require('./sql/SQLiteWorker');
const PluginBaseWorker = require('./PluginBaseWorker');
const FileWorker = require('./FileWorker');
const S3Worker = require('./file/S3Worker');
const PersonWorker = require('./PersonWorker');
const { analyzeTypeToParquet, cleanColumnName, bool } = require('../utilities');
const analyzeStream = require('../utilities/analyze');

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
  if (this.knex) this.knex.destroy();
};

Worker.prototype.getMetadata = async function ({ directory }) {
  if (!directory) throw new Error('directory is required');
  const fileWorker = new FileWorker(this);
  try {
    const filename = `${directory}${directory.endsWith('/') ? '' : '/'}metadata.json`;
    if (filename.startsWith('/')) {
      // see if it exists or fast fail
      // The stream method below doesn't handle fast fails as well
      // await fsp.stat(filename);
    }
    const { stream } = await fileWorker.stream({ filename });
    const metadata = await StreamConsumers.json(stream);
    return { metadata };
  } catch (e) {
    const s = e.toString();
    if (s.indexOf('NoSuchKey') >= 0 || s.indexOf('ENOENT') >= 0) {
      return { metadata: {}, noFile: true };
    }
    // throw e;
    return { metadata: {} };
  }
};
Worker.prototype.getMetadata.metadata = {
};

Worker.prototype.listMissingIdFiles = async function (options) {
  let { directory } = options;
  if (!directory) directory = `${this.store_path}/${this.accountId}/stored_inputs`;
};
Worker.prototype.listMissingIdFiles.metadata = {

};

Worker.prototype.id = async function (options) {
  const worker = this;

  const {
    defaultEntryType, filename,
  } = options;
  const parts = filename.split('/');
  let { inputId, pluginId } = options;
  if (!inputId) {
    const uuid = parts[parts.length - 2];
    if (uuidIsValid(uuid)) {
      inputId = uuid;
    } else {
      throw new Error('id requires an inputId, and the directory from the filename is not correct');
    }
  }
  if (!pluginId) {
    const { data: inputData } = await this.query({ sql: 'select plugin_id from input where id=?', values: [inputId] });
    const input = inputData[0];
    if (input?.plugin_id) {
      pluginId = input.plugin_id;
    } else {
      // check the filename
      const testPluginId = parts[parts.length - 4];
      if (uuidIsValid(testPluginId)) {
        pluginId = testPluginId;
      } else {
        const remote_plugin_id = testPluginId.split(':').slice(-1)[0];
        const { data: pluginData } = await this.query({ sql: 'select id from plugin where remote_plugin_id=?', values: [remote_plugin_id] });
        pluginId = pluginData?.[0]?.id;
        if (!pluginId) {
          throw new Error(`pluginId is required to identify new people, was not specified, and could not find input id ${inputId}, nor remote_plugin_id ${remote_plugin_id} in the database to track it down`);
        }
      }
    }
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
    if (['remote_entry_uuid', 'input_id', 'identifiers', 'entry_type', 'source_code', 'remote_input_id', 'remote_input_name'].indexOf(b.name) >= 0) return a;
    a[b.name] = {
      type: b.type,
      parquetType: pq.type,
      parquetMap: pq.map,
      compression: 'GZIP',
    };
    return a;
  }, initialSchema);

  const parquetSchemaDefinition = {};
  Object.entries(fieldMap).forEach(([name, obj]) => {
    parquetSchemaDefinition[name] = {
      type: obj.parquetType,
      compression: obj.compression || 'UNCOMPRESSED',
    };
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
  // for debugging
  // writer.setRowGroupSize(1);

  const batcher = this.getBatchTransform({ batchSize: 500 }).transform;
  let records = 0;
  let batches = 0;
  let minTimestamp = null;
  let maxTimestamp = null;
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
          if (b.ts) {
            const ts = new Date(b.ts).getTime();
            if (!minTimestamp || ts < minTimestamp) minTimestamp = ts;
            if (!maxTimestamp || ts > maxTimestamp) maxTimestamp = ts;
          }

          try {
            await writer.appendRow(row);
          } catch (e) {
            debug(`Error processing file ${filename}`);
            debug(`Working on writing ${outputFile}`);
            debug('Parquet definition:', JSON.stringify(parquetSchemaDefinition, null, 4));
            debug('File analysis:', fields);
            debug('Last entry:', row);
            await writer.close();
            throw e;
          }
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
    await s3.put({ filename: outputFile, directory });
    const file = outputFile.split('/').pop();
    return {
      originalFields: fields,
      parquestSchema: parquetSchemaDefinition,
      idFilename: `${directory}/${file}`,
      sourceIdFilename: outputFile,
      records,
      inputId,
      minTimestamp,
      maxTimestamp,
    };
  }
  return {
    originalFields: fields,
    parquestSchema: parquetSchemaDefinition,
    idFilename: outputFile,
    records,
    inputId,
    minTimestamp,
    maxTimestamp,
  };
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
Worker.prototype.idFiles = async function (options) {
  let arr = options.fileArray;
  if (typeof arr === 'string') arr = JSON.parse(arr);
  if (!Array.isArray(arr))arr = [options];
  const output = [];
  const directories = {};
  // check for the pluginId if not specified
  for (const o of arr) {
    const { inputId } = o;
    if (!inputId) {
      debug(`No inputId specified in ${JSON.stringify(o, null, 4)}`);
      throw new Error('inputId must be specified');
    }
    if (!o.pluginId) {
      const { data } = await this.query(`select plugin_id from input where id=${this.escapeValue(inputId)}`);
      o.pluginId = data?.[0]?.plugin_id;
      if (!o.pluginId) throw new Error(`pluginId must be specified, and cannot be found in the database for inputId${inputId}`);
    }
  }

  // eslint-disable-next-line no-restricted-syntax
  for (const o of arr) {
    const {
      inputId, pluginId, filename,
    } = o;
    const directory = filename.split('/').slice(0, -1).join('/');
    let metadata = {};
    try {
      metadata = (await this.getMetadata({ directory })).metadata;
    } catch (e) {
      debug(e);
    }

    const { inputType, remoteInputId, remoteInputName } = metadata;

    const {
      idFilename, minTimestamp, maxTimestamp,
    } = await this.id({
      filename, inputId, pluginId,
    });

    await this.insertFromStream({
      table: 'input',
      stream: [{
        id: inputId,
        plugin_id: pluginId,
        input_type: inputType,
        remote_input_id: remoteInputId,
        remote_input_name: remoteInputName,
        data_path: directory,
      }],
      upsert: true,
    });
    const sql = `update input set 
    min_timeline_ts=CASE WHEN min_timeline_ts is null then ${this.escapeDate(minTimestamp)}
    else LEAST(input.min_timeline_ts,${this.escapeDate(minTimestamp)}) end,
    max_timeline_ts=CASE WHEN max_timeline_ts is null then ${this.escapeDate(maxTimestamp)}
    else GREATEST(input.max_timeline_ts,${this.escapeDate(maxTimestamp)}) end
    WHERE id=${this.escapeValue(inputId)}`;
    // debug('Upserting input with', x, 'then updating with ', sql);

    await this.query(sql);

    directories[idFilename.split('/').slice(0, -1).join('/')] = true;
    const outputVals = {
      inputId,
      idFilename,
      sourceFilename: filename,
    };
    output.push(outputVals);
  }

  return {
    fileArray: output,
    directoryArray: Object.keys(directories).map((directory) => ({ directory })),
  };
};
Worker.prototype.idFiles.metadata = {
  options: {
    fileArray: {},
    filename: {},
    inputId: {},
    pluginId: {},
  },
};

Worker.prototype.createDetailTable = async function (options) {
  await this.connect();
  const fworker = new FileWorker(this);
  const stream = await fworker.fileToObjectStream(options);
  const analysis = await analyzeStream(stream);
  const table = options.table || `temp_${new Date().toISOString().replace(/[^0-9]/g, '_')}`.slice(0, -1);

  const indexes = [{
    primary: true,
    columns: 'id',
  }];

  const columns = [
    {
      name: 'id',
      type: 'id_uuid',
    },
  ].concat(
    analysis.fields.map((f) => {
      let { name } = f;
      if (['id',
        'ts',
        'remote_person_id',
        'source_code',
        'input_id',
        'remote_input_id',
        'remote_input_name',
        'entry_type',
        'remote_entry_id',
        'remote_entry_uuid',
        'created_at', // standard field names
        'modified_at', // standard field names
      ].indexOf(name) >= 0) {
        return false;
      }

      name = cleanColumnName(name);
      return {
        isKnexDefinition: true,
        name,
        ...this.deduceColumnDefinition(f, this.version),
      };
    }).filter(Boolean),
  );
  debug(columns);
  return this.createTable({
    table, columns, indexes,
  });
};
Worker.prototype.createDetailTable.metadata = {
  options: {
    filename: {},
    table: {},
  },
};

Worker.prototype.loadTimelineTables = async function (options) {
  const loadTimeline = bool(options.loadTimeline, false);
  const loadTimelineDetails = bool(options.loadTimelineDetails, false);
  const { timelineDetailTable } = options;
  if (loadTimelineDetails && !loadTimeline) throw new Error('Cowardly refusing to load details table without loadTimeline as well');
  if (loadTimelineDetails && !timelineDetailTable) throw new Error('Cowardly refusing to load details table without timelineDetailTable as well');
  if (!loadTimeline) return options;

  let arr = options.fileArray;
  if (typeof arr === 'string') arr = JSON.parse(arr);
  if (!Array.isArray(arr))arr = [options];
  const fileArray = [];
  for (const o of arr) {
    const { inputId, pluginId, idFilename } = o;
    if (!idFilename) throw new Error('No idFilename specified');
    const output = {
      inputId,
      pluginId,
      idFilename,
    };
    output.timelineResults = await this.loadTimeline({
      filename: idFilename,
      inputId,
    });
    if (options.loadTimelineDetail) {
      output.detailResults = await this.loadTimelineDetails({
        filename: idFilename,
        table: timelineDetailTable,
      });
    }
    fileArray.push(output);
  }
  const o = { fileArray };

  // Pass through if there is a directory array
  if (options.directoryArray) o.directoryArray = options.directoryArray;
  return o;
};

Worker.prototype.loadTimelineTables.metadata = {
  options: {
    fileArray: { description: 'Array of id filenames with other data' },
    idFilename: {},
    loadTimeline: { description: 'Whether to load the timeline table or not, default false' },
    loadTimelineDetail: { description: 'Whether to load the timeline detail table or not, default false' },
    timelineDetailTable: { description: 'Load this details table' },
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
      let { directory } = d;
      while (directory.slice(-1) === '/')directory = directory.slice(0, -1);
      files = (await fileWorker.list({ directory }))
        .filter((f) => f.name.endsWith('.id.parquet'))
        .map((f) => ({ idFilename: `${directory}/${f.name}` }));
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
    statistics.byTypeByDate = (await sqliteWorker.query(`
        select strftime('%Y-%m-%d', ts/1000, 'unixepoch') as date,
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
    const result = {};
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
      result.statistics = statistics;
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
  const { stream: inputStream } = await fileWorker.fileToObjectStream({ ...options, columns });

  const stream = inputStream.pipe(
    new Transform({
      objectMode: true,
      async transform(item, enc, cb) {
        if (!uuidIsValid(item.id)) {
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
  const { table } = options;

  try {
    const { columns } = await this.describe({ table });
    const idColumn = columns.find((c) => (c.name === 'id'));
    if (!idColumn
        || (['id_uuid'].indexOf(idColumn.type) < 0)) {
      debug(`Existing columns:${JSON.stringify(columns, null, 4)}`);
      throw new Error(`timeline detail table ${table} needs an id column of type uuid, found type ${idColumn?.type}`);
    }
    const fileWorker = new FileWorker(this);
    const { stream } = await fileWorker.fileToObjectStream({ ...options, columns });
    return this.insertFromStream({
      table: options.table, stream, upsert: true,
    });
  } catch (e) {
    if (e.code === 'DOES_NOT_EXIST') {
      return this.createAndLoadTable({ ...options, primary: 'id' });
    }
    throw e;
  }
};
Worker.prototype.loadTimelineDetails.metadata = {
  options: {
    stream: {},
    filename: {},
    columns: {},
  },
};

Worker.prototype.ensureTimelineDetailSummary = async function (options) {
  const { table } = options;

  const { columns } = await this.describe({ table });
  const ignore = ['id', 'ts', 'input_id', 'entry_type_id', 'source_code_id', 'person_id', 'remote_input_id', 'remote_input_name'];

  const entryType = `case ${Object.entries(TIMELINE_ENTRY_TYPES)
    .filter(([, value]) => typeof value === 'number')
    .map(([key, value]) => `when timeline.entry_type_id=${value} then ${this.escapeValue(key)}`).join(' \n')}
    else 'N/A' end as entry_type`;
  const sql = `select
  timeline.id AS id,
  timeline.ts AS ts,
  timeline.entry_type_id,
  ${entryType},
  timeline.created_at AS created_at,
  timeline.person_id AS person_id,
  timeline.input_id AS input_id,
  input.input_type AS input_type,
  input.remote_input_id AS remote_input_id,
  input.remote_input_name AS remote_input_name,
  plugin.path AS plugin_path,
  plugin.name AS plugin_name,
  plugin.nickname AS plugin_nickname,
  plugin.remote_plugin_id AS plugin_remote_id,
  dict.source_code_id AS source_code_id,
  dict.source_code AS source_code,
  ${columns.filter((d) => ignore.indexOf(d.name) < 0).map((d) => this.escapeColumn(d.name)).join(',')}
from ${table} d
    JOIN timeline on (d.id = timeline.id)
    JOIN input on (timeline.input_id = input.id)
    JOIN plugin on (input.plugin_id = plugin.id)
    left JOIN source_code_dictionary dict on (timeline.source_code_id = dict.source_code_id)`;
  return this.ensureView({ table: `${table}_summary`, sql });
};
Worker.prototype.ensureTimelineDetailSummary.metadata = {
  table: {},
};

module.exports = Worker;
