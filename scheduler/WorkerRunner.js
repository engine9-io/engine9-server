const async = require('async');
const util = require('util');
const path = require('path');
const fs = require('fs');
const JSON5 = require('json5');// Useful for parsing extended JSON
const { RateLimiter } = require('limiter');
const minimist = require('minimist');
const debug = require('debug')('WorkerRunner');
const debugError = require('debug')('error:WorkerRunner');
const { relativeDate } = require('../utilities');

let config = null;
try {
  // eslint-disable-next-line global-require
  config = require('../account-config.json');
} catch (e) {
  debug(e);
  throw new Error('Error loading config.json file -- make sure to create one from config.template.json before running');
}

let prompt = null;
let winston = null;

const allNames = process.argv.slice(3).map((x) => x.split(/[=]/g)[0].replace(/^[-]*/, '')); // this treats all command line options as strings, not numbers
const argv = minimist(process.argv.slice(2), { string: allNames });

function WorkerRunner() {}

function getFiles(dir, files = []) {
  fs.readdirSync(dir).forEach((f) => {
    const p = path.resolve(dir, f);
    if (fs.statSync(p).isDirectory()) {
      getFiles(p, files);
    } else {
      files.push(p);
    }
  });

  return files;
}

WorkerRunner.prototype.getWorkerPath = function getWorkerPath(options, callback) {
  debug('Getting worker path with ', argv);
  let pathLookup = argv._[0];
  if (!pathLookup) return callback('No path specified in command line');
  pathLookup = pathLookup.toLowerCase();
  if (pathLookup === 'sql')pathLookup = 'sqlworker';// optimization for sql specific worker
  const d = path.resolve(__dirname, '../workers');
  debug('Looking for workers in path:', d);
  const availableWorkerPaths = getFiles(d);
  const paths = availableWorkerPaths.filter((p) => p.toLowerCase().indexOf(pathLookup) >= 0);
  if (paths.length === 0) {
    debug(`No worker path found that matches ${pathLookup}`);
    return callback(`Could not find a worker that matches ${pathLookup}`);
  }
  debug('Found worker paths:', paths);
  const p = paths[0];
  if (paths.length > 1) debug(`Multiple paths found, using ${p}`); // notify, but don't error, for convenience
  debug('Returning path ', p);
  return callback(null, p);
};

WorkerRunner.prototype.getWorkerConstructor = function getWorkerConstructor(options, callback) {
  const runner = this;
  return async.autoInject({
    workerPath: (cb) => runner.getWorkerPath(options, cb),
    WorkerConstructor: (workerPath, cb) => {
      debug(`Getting constructor by requiring ${workerPath}`);
      let WorkerConstructor = null;
      try {
        // eslint-disable-next-line
        WorkerConstructor = require(workerPath);
      } catch (e) {
        debugError(e);
        const msg = (e.message || e.toString());
        if (msg.indexOf('Cannot find module') >= 0) {
          debug(e.stack);
          debug(`Could not find ${path.module}, continuing`);
        } else {
          debug(`Error compiling module ${options.path}, ${msg}`);
          debug(e.stack);
        }

        return cb(e);
      }
      if (path.workerSubpath) {
        path.workerSubpath.forEach((d) => {
          debug(`Looking for subpath ${d}`);
          if (WorkerConstructor[d]) {
            WorkerConstructor = WorkerConstructor[d];
            return;
          }
          if (!WorkerConstructor) throw new Error(`Could not find part ${d}`);
          if (typeof WorkerConstructor === 'function') {
            WorkerConstructor = WorkerConstructor(d);
          }
        });
      }
      if (typeof WorkerConstructor !== 'function') {
        throw new Error(`Not a valid workerInstance -- it is of type ${typeof WorkerConstructor} with keys ${Object.keys(WorkerConstructor)}`);
      }
      const fullName = path.module + (path.workerSubpath ? `.${path.workerSubpath.join('.')}` : '');
      WorkerConstructor.path = fullName;
      return cb(null, WorkerConstructor);
    },
  }, (e, { WorkerConstructor } = {}) => callback(e, WorkerConstructor));
};

const loggers = {};

WorkerRunner.prototype.getLogPath = function getLogPath(job) {
  if (!job.jobId) throw new Error(`Could not find jobId in ${JSON.stringify(job)}`);
  if (!job.job_list_id) throw new Error(`Could not find job_list_id in ${JSON.stringify(job)}`);

  const logDir = process.env.ENGINE9_LOG_DIR || '/var/log/engine9';
  return `${logDir + path.sep}jobs${path.sep}${job.accountId}_${job.job_list_id}_${job.jobId}.txt`;
};

WorkerRunner.prototype.getLogger = function getLogger(job) {
  const runner = this;
  const logPath = runner.getLogPath(job);

  if (!loggers[path]) {
    debug(`Creating logfile ${path}`);
    // eslint-disable-next-line global-require
    winston = winston || require('winston');
    const winstonLogger = winston.createLogger({
      format: winston.format.simple(),
      transports: [
        new (winston.transports.File)({ level: 'debug', filename: logPath, json: false }),
      ],
    });
    loggers[path] = winstonLogger;
  }

  function log(m) {
    let stringVal = m;
    if (m.message) stringVal = m.message;

    loggers[path].info(stringVal);
  }

  return log;
};

WorkerRunner.prototype.getMethod = function getMethod(WorkerConstructor, callback) {
  const methods = [];
  if (!WorkerConstructor?.prototype) return callback(`Worker must be a function, it is ${typeof WorkerConstructor}`);

  Object.entries(WorkerConstructor.prototype).forEach(([i, v]) => {
    if (typeof v === 'function' && v.metadata) {
      methods.push({ name: i, metadata: v.metadata });
    }
  });

  if (methods.length === 0) {
    return callback(`There are no available method for this workerInstance, with prototype items:${Object.keys(WorkerConstructor.prototype)}`);
  }
  let method = null;
  if (argv._[1]) {
    const n = argv._[1].toLowerCase();
    // debug("A method was specified:",n);
    method = methods.filter((m) => m.name.toLowerCase() === n);

    if (method.length > 1) return callback(`Multiple methods match ${argv._[1]}: ${method.map((d) => d.name).join()}`);
    if (method.length === 0) {
      debug('Available methods:', methods.map((m) => m.name));
      return callback(`Could not find method ${argv._[1]}`);
    }

    method[0].is_specified = true;

    // debug("Returning method",n);
    return callback(null, method[0]);
  }
  /* Below is to prompt the CLI user for the method name */
  const methodOpts = { type: 'string', description: 'Method', required: true };
  methodOpts.default = methods[methods.length - 1].name;
  const s = methods.map((m) => m.name);
  s.sort();
  debug(`Available methods:\n${s.join('\n')}`);

  // eslint-disable-next-line global-require
  if (!prompt) prompt = require('prompt');

  return prompt.get({
    properties: {
      method: methodOpts,
    },
  }, (err, result) => {
    if (err) throw (err);

    const n = result.method.toLowerCase();

    method = methods.filter((m) => m.name.toLowerCase() === n);

    if (method.length > 1) return callback(`Multiple methods match ${argv._[1]}: ${method.map((d) => d.name).join()}`);
    if (method.length === 0) return callback(`Could not find method ${argv._[1]}`);

    return callback(null, method[0]);
  });
};

function getStdIn(cb) {
// ttys used /dev/tty, which is NOT available when running a cron job, thus ttys does NOT work
// We must do EITHER TTY mode, or inline mode and throw errors

  if (process.stdin.isTTY) {
    debug('This is a TTY, returning blank options');
    return cb(null, {});
  }
  debug('This is not a TTY, resuming stdin input');

  process.stdin.resume();
  // process.stdin.setEncoding('utf8');
  let toParse = '';
  process.stdin.on('data', (data) => {
    toParse += data;
  });

  return process.stdin.on('end', () => {
    let output = {};
    if (toParse) {
      output = JSON5.parse(toParse);
    }
    return cb(null, output);
  });
}

WorkerRunner.prototype.getOptionValues = function getOptionValues(method, callback) {
  getStdIn((e, stdInOptions) => {
    if (e) throw e;

    const options = {};

    // First do stdin
    Object.entries(stdInOptions).forEach(([k, v]) => {
      options[k] = v;
    });

    // Then do other command line items
    Object.entries(argv).forEach(([i, v]) => {
      if (i === '_' || i.indexOf('$') === 0) return;
      options[i] = v;
    });
    // Prefill any defaults
    Object.entries(method.metadata.options || {}).forEach(([i]) => {
      if (!options[i] && method?.metadata?.options?.[i]?.default) {
        options[i] = method.metadata.options[i].default;
      }
    });

    const stillRequired = Object.keys(method.metadata.options || {}).filter((d) => {
      const o = method.metadata.options[d];
      const v = ((!o.required) ? false : !options[d]);
      return v;
    });

    if (stillRequired.length > 0) debug('Some fields are still required:', stillRequired.join());

    // If there's no user tty, and there's still required fields, throw an error
    if (!process.stdout.isTTY) {
      if (stillRequired.length > 0) {
        // records=0 should still run the job, because required fields get a pass at that point
        if (options.records === 0) {
          // Do nothing
        } else {
          return callback(`Not a tty, and not all required fields specified, specifically:${stillRequired.join()}`);
        }
      }
      // debug("Not a TTY, returning back options:",Object.keys(options));
      return callback(null, options);
    }

    // Human execution below -- not a tty

    /* optimization to just run it if a method is specified */
    if (method.is_specified) {
      if (stillRequired.length > 0) {
        debug('Method was specified, still require fields ', stillRequired);
        // need to request fields from user
      } else {
        debug('All required fields specified, returning options');
        return callback(null, options);
      }
    }

    debug('No method specified, requesting from user');

    // Okay, at this point a method hasn't been specified, or there are still required fields

    const requestFromUser = JSON.parse(JSON.stringify({}, method.metadata.options));

    // Type is NOT respected on the command line -- 'prompt' is too specific on boolean and dates
    Object.entries(requestFromUser).forEach(([k, _f]) => {
      const f = _f;
      delete f.type;
      if (f.description) {
        f.description = `[${k}] ${f.description}`;
      }
      if (options[k]) f.default = options[k];
    });
    // eslint-disable-next-line global-require
    if (!prompt) prompt = require('prompt');
    return prompt.get({
      properties: requestFromUser,
    }, (err, userOptions) => {
      if (err) throw err;

      debug('Received user options:', userOptions);
      Object.entries(userOptions).forEach(([k, v]) => {
        options[k] = v;
      });

      // handle boolean values truthiness
      Object.keys(options).forEach((i) => {
        if (options[i] === 'false') options[i] = false;
      });

      delete options.accountId;

      return callback(null, options);
    });
  });
};

function getAccountIds() {
  if (argv.account_id) throw new Error('account_id is not an allowed option');
  if (argv.account_ids) throw new Error('account_ids is not an allowed option');
  let accountIds = null;
  if (argv.accountId) {
    accountIds = String(argv.accountId);
  } else if (argv.a) {
    accountIds = String(argv.a);
  } else if (process.env.ENGINE9_ACCOUNT_ID) {
    accountIds = process.env.ENGINE9_ACCOUNT_ID;
  } else {
    accountIds = 'engine9';
  }
  accountIds = accountIds.split(',');
  accountIds.forEach((accountId) => {
    if (!accountId.match(/^[a-zA-Z0-9_-]+$/)) throw new Error(`invalid accountId=${accountId}`);
  });
  // don't need this in the options
  delete argv.accountId;
  delete argv.a;
  return accountIds;
}

WorkerRunner.prototype.getWorkerEnvironment = function getWorkerEnvironment(options, callback) {
  if (!options) throw new Error('getWorkerEnvironment requires options');
  const accountEnvironment = config.accounts?.[options.accountId] || {};
  // don't print the environment, could have credentials
  debug('Using environment with keys:', Object.keys(accountEnvironment));
  accountEnvironment.accountId = options.accountId;
  return (typeof callback === 'function') ? callback(null, accountEnvironment) : accountEnvironment;
};

WorkerRunner.prototype.run = function () {
  const accountIds = getAccountIds();
  function processCallback(_e, uncaught) {
    debug('Processing callback');
    if (_e) {
      let e = _e;
      if (!e.stack && typeof e === 'object')e = JSON.stringify(e, null, 4);
      let s = ((uncaught) ? (`${new Date().toString()}: Uncaught Exception: `) : '') + (e.stack || e);
      if (process.stdout.isTTY) {
        s = `\u001b[31m ${s} \u001b[0m `;
      }
      // eslint-disable-next-line no-console
      console.error(s);
      if (process.send) {
        process.send({ message_type: 'error', data: e.stack || e }, () => {
          process.exit(-1);
        });
      } else {
        process.exit(-1);
      }
    }
    // Add a tenth of a second to complete writing to stdout
    // In MacOS without this the process exits before all content is written to the output
    setTimeout(() => {
      process.exit(0);
    }, 100);
  }

  if (accountIds.length === 1) {
    debug('Starting Runner with account id ', accountIds[0]);
    return this.runAccount(accountIds[0], processCallback);
  }
  debug(`Starting ${accountIds.length} Runners with account ids ${accountIds.join(',')}`);
  const status = { output: [], success: [], error: [] };
  return async.eachSeries(
    accountIds,
    (accountId, acb) => this.runAccount(accountId, (e, output) => {
      if (e) status.error.push(accountId);
      else {
        status.success.push(accountId);
        status.output.push({ accountId, output });
      }
      debug(`Finished ${accountId}, calling account callback`);
      acb();
    }),
    (e) => {
      if (e) throw e;
      // eslint-disable-next-line no-console
      console.log(status);
      processCallback();
    },
  );
};

WorkerRunner.prototype.runAccount = function runAccount(accountId, callback) {
  const runner = this;
  debug(`Processing accountId=${accountId} in environment ${process.env.NODE_ENV} with debug ${process.env.DEBUG}`);

  process.on('uncaughtException', (e) => callback(e, true));

  process.on('SIGHUP', () => {
    debug(`${new Date().toString()}SIGHUP received`);
    if (process.send) {
      process.send({ message_type: 'error', data: { message: 'Stopped by system' } }, () => {
        process.exit(-3);
      });
    } else {
      process.exit(-3);
    }
  });

  process.on('SIGTERM', () => {
    debug(`${new Date().toString()}SIGTERM received`);
    if (process.send) {
      process.send({ message_type: 'error', data: { level: 'CRITICAL', message: 'Killed by system' } }, () => {
        process.exit(-2);
      });
    } else {
      process.exit(-2);
    }
  });

  process.on('SIGTSTP', () => {
    debug(`${new Date().toString()}SIGTSTP received`);
    if (process.send) {
      process.send({ message_type: 'error', data: { level: 'CRITICAL', message: 'Killed by user - TStop signal' } }, () => {
        process.exit(-2);
      });
    } else {
      process.exit(-2);
    }
  });

  /*
  For some reason something has been sending SIGQUIT signals
  -- allow for restarts on these, may be system busy-ness
*/
  process.on('SIGQUIT', () => {
    debug(`${new Date().toString()}SIGQUIT received at ${new Date()}`);
    if (process.send) {
      process.send({ message_type: 'error', data: { message: 'Killed by system - Quit signal' } }, () => {
        debug(`SIGQUIT exiting job at ${new Date()}`);
        process.exit(-2);
      });
    } else {
      debug(`SIGQUIT exiting job at ${new Date()}`);
      process.exit(-2);
    }
  });

  process.on('SIGINT', () => {
    debug(`${new Date().toString()}: SIGINT received`);
    if (process.send) {
      process.send({ message_type: 'error', data: { message: 'Killed because of a timeout' } }, () => {
        process.exit(-2);
      });
    } else {
      process.exit(-2);
    }
  });
  async.autoInject({
    WorkerConstructor: (cb) => runner.getWorkerConstructor({ accountId }, cb),
    environment: (WorkerConstructor, cb) => {
      runner.getWorkerEnvironment({ accountId }, cb);
    },
    method: (environment, WorkerConstructor, cb) => {
      if (!WorkerConstructor) {
        return cb({
          level: 'CRITICAL',
          message: 'Did not get valid constructor',
        });
      }
      return runner.getMethod(WorkerConstructor, cb);
    },
    _options: (method, cb) => runner.getOptionValues(method, cb),
    _workerInstance: (_options, WorkerConstructor, environment, instanceCallback) => {
      try {
        // Call the constructor, which should take needed items out of the environment
        const workerInstance = new WorkerConstructor(environment);
        return instanceCallback(null, workerInstance);
      } catch (e) {
        debug('Error creating the worker instance:', e);
        return instanceCallback(e);
      }
    },
    assignCommonMethods: (_workerInstance, method, cb) => {
      const progressLimiter = new RateLimiter(1, 3000);
      const workerInstance = _workerInstance;
      workerInstance.accountId = accountId;
      workerInstance.progress = function progress(prog) {
        // Okay, we need to throttle this, so we don't have too many items
        let o = null;
        if (typeof prog === 'string') {
          o = { message: prog };
        } else {
          o = JSON.parse(JSON.stringify(prog));
        }

        if (!o.percent_complete) {
          workerInstance.local_progress_total = workerInstance.local_progress_total || 0;
          workerInstance.local_progress_complete = workerInstance.local_progress_complete || 0;
          if (parseInt(prog.incomplete, 10) === prog.incomplete) {
            workerInstance.local_progress_total += parseInt(prog.incomplete, 10);
            delete o.incomplete;
          }
          if (parseInt(prog.complete, 10) === prog.complete) {
            workerInstance.local_progress_complete += parseInt(prog.complete, 10);
            delete o.complete;
          }
          o.percent_complete = (100 * workerInstance.local_progress_complete)
            / workerInstance.local_progress_total;
        }

        debug(new Date().toString(), 'Progress: ', o);
        // Don't overwhelm our progress tracker -- 1 per 3 seconds
        if (progressLimiter.tryRemoveTokens(1)) {
          if (process.send) {
            process.send({ message_type: 'progress', data: o });
          }
        } else {
          // do nothing
        }
      };

      workerInstance.log = debug;

      workerInstance.jobId = argv._.find((d) => d.indexOf('_jobId') === 0);
      if (workerInstance.jobId)workerInstance.jobId = workerInstance.jobId.slice(workerInstance.jobId.indexOf('=') + 1);
      cb();
    },
  }, (configError, {
    _options, method, _workerInstance,
  } = {}) => {
    const workerInstance = _workerInstance;
    let options = _options;

    if (configError) {
      debug('There was a configuration error, returning critical error');
      let error = configError;
      if (typeof error === 'string') {
        error = {
          level: 'CRITICAL',
          message: error,
        };
      }
      return callback(error);
    }
    function runOnce() {
      let hasError = false;

      /*
        If it's async, await the response and check for a modify output
      */

      async function checkAsync(cb) {
        if (util.types.isAsyncFunction(workerInstance[method.name])) {
          try {
            debug(`Running async ${method.name} with options `, Object.keys(options));
            const l = workerInstance[method.name].length;
            if (l > 1) throw new Error(`command line async functions must take zero or one parameter, '${method.name}' has ${l}`);
            const response = await (workerInstance[method.name](options));
            if (response === undefined || response === null) throw new Error('No return value from method -- be sure to return something');
            if (response.modify) return cb(null, null, response.modify);
            return cb(null, response);
          } catch (e) {
            return cb(e);
          }
        }
        debug(`Running non-async ${method.name} with options `, Object.keys(options));
        return workerInstance[method.name](options, cb);
      }
      const start = new Date().getTime();
      checkAsync((_e, _output, _modify) => {
        const runtime = new Date().getTime() - start;
        const modify = _modify;

        debug(`Exited running ${method.name}`);
        if (hasError) {
          workerInstance.log({ type: 'warning', message: `Output specified, but already errored:${JSON.stringify(_output || {})}` });
          workerInstance.log({ type: 'error', message: 'Error has already been called' });
          return null;
        }

        if (_e) {
          const e = (typeof _e === 'string') ? new Error(_e) : _e;
          debug(`Error executing job ${method.name}`, 'Error type:', typeof e);
          hasError = true;
          return callback(e);
        }
        if (modify) {
          debug('A modification was specified:', modify);
          try {
            JSON.stringify(modify);
          } catch (e) {
            // eslint-disable-next-line no-undef
            debug('Modifying job, but there was an unserializable part of the modify call:', modify);
            // eslint-disable-next-line no-undef
            return callback(e);
          }
          // If we're restarting it, and status isn't specified, assume pending
          if (modify.start_after_timestamp) {
            if (!modify.status) modify.status = 'pending';
            const ts = modify.start_after_timestamp;
            if (typeof ts === 'string' && ts.indexOf('+') === 0) modify.start_after_timestamp = relativeDate(ts);
          }
          workerInstance.log({ type: 'modify', message: modify });

          // eslint-disable-next-line no-shadow
          const output = {
            job_response_type: 'modify',
            modify,
          };
          modify.options = modify.options || {};

          if (process.send) {
            debug(JSON.stringify(output));
            process.exit(0);
          } else {
            let timeout = 2000;
            if (modify.start_after_timestamp) {
              timeout = (new Date(relativeDate(modify.start_after_timestamp)).getTime()
                - new Date().getTime());
            }
            if (modify.options) options = Object.extend({}, options, modify.options);

            setTimeout(runOnce, timeout);
          }
          return callback();
        }
        // Try to parse the output, looking for circular structure
        try {
          JSON.stringify(_output);
        } catch (e) {
          debug(`Completed method ${method.name} but there was an unserializable part of the response:`, _output);
          hasError = true;
          return callback(e);
        }
        /*
            Add in standard workerInstance stored variables
            */
        const metadata = { ...(_output || {}).metadata || {} };
        if (workerInstance.http_counter) {
          metadata.http_counter = workerInstance.http_counter;
        }

        if (workerInstance.sql_counter) {
          metadata.sql_counter = workerInstance.sql_counter;
        }

        // metadata.resolved_options = options;

        if (_output.records !== undefined) {
          metadata.records = _output.records;
        }

        metadata.runtime = runtime;
        const response = _output;
        // Don't assign metadata all the time to the output,
        // dirties up the output for arrays,files, etc. Just log it
        debug('Output Metadata:', metadata);
        // response.metadata = metadata;
        if (process.stdout.isTTY) {
          // eslint-disable-next-line no-console
          console.log(util.inspect(response, { colors: true, depth: 6, maxArrayLength: 1000 }));
        } else {
          // eslint-disable-next-line no-console
          console.log(JSON.stringify(response, null, 4));
        }
        return callback(null, response);
      });
    }
    try {
      return runOnce();
    } catch (e) {
      debug('Error caught running method', method);
      return callback(e);
    }
  });
};
function end(e, d) {
  if (e) {
    throw e;
  }

  if (d) {
    // eslint-disable-next-line no-console
    console.log(JSON.stringify(d, null, 4));
  }

  process.exit();
}

if (require.main === module) {
  if (!process.env.DEBUG) {
    // eslint-disable-next-line
    console.error('No DEBUG environment variable set, no likely output');
  }
  if (argv._.length === 1 && (argv.a || argv.accountId)) {
    // just setting an account id, so return
    end();
  } else {
    const r = new WorkerRunner();
    r.run();
  }
} else {
  // may want to use the configuration loader, so this can be included
  // throw new Error('WorkerRunner should not be executed');
}

module.exports = WorkerRunner;
