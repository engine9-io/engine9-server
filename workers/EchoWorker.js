/*
 Do nothing but echo back the input options, except the 'auth' options
*/
const debug = require('debug')('EchoWorker');
const { relativeDate } = require('../utilities');

const BaseWorker = require('./BaseWorker');

function Worker(worker) {
  BaseWorker.call(this, worker);
}

require('util').inherits(Worker, BaseWorker);

Worker.metadata = {
  alias: 'echo',
};

Worker.prototype.echo = function echo(options, callback) {
  const obj = JSON.parse(JSON.stringify(options));
  obj.last_run = new Date();

  const seconds = options.seconds || 1;
  this.log('Starting echo...');
  const message = `Echo for account ${this.accountId} working.....`;
  setTimeout(() => { this.progress({ message, percent_complete: 10 }); }, seconds * 100);
  setTimeout(() => { this.progress({ message, percent_complete: 30 }); }, seconds * 300);
  setTimeout(() => { this.progress({ message, percent_complete: 50 }); }, seconds * 500);
  setTimeout(() => { this.progress({ message, percent_complete: 70 }); }, seconds * 700);
  setTimeout(() => { this.progress({ message, percent_complete: 90 }); }, seconds * 900);
  setTimeout(() => {
    this.log('Echo completing');
    callback(null, obj);
  }, seconds * 1000);
  debug('Returning from echo, no callback');
};
Worker.prototype.echo.metadata = {
  options: {
    seconds: {},
    foo: {},
  },
};

Worker.prototype.async = async function asyncFunc(options) {
  function timeout(ms) {
    return new Promise((resolve) => { setTimeout(resolve, ms); });
  }
  const seconds = parseInt(options.seconds || 0, 10);
  debug(`Async function waiting ${seconds}`);
  await timeout(seconds * 1000);
  const obj = JSON.parse(JSON.stringify(options));
  obj.last_run = new Date();
  return obj;
};
Worker.prototype.async.metadata = {
  options: {
    seconds: {},
  },
};
Worker.prototype.asyncError = async function echo(options) {
  function timeout(ms) {
    return new Promise((resolve) => { setTimeout(resolve, ms); });
  }
  await timeout(parseInt(options.seconds || 1, 10) * 1000);
  throw new Error('Expected asyncError');
};
Worker.prototype.asyncError.metadata = {
  options: {
    seconds: {},
  },
};

Worker.prototype.info = function info(options, callback) {
  debug('Returning metadata about this worker');
  return callback(null, Worker.metadata);
};
Worker.prototype.info.metadata = {
  options: {},
};

Worker.prototype.logTest = function logTest(options, callback) {
  const worker = this;
  const msg = options.message || `Sample message:${new Date()}`;

  debug(`console.error:${msg}`);
  debug(`debug:${msg}`);
  worker.progress(`progress:${msg}`);
  worker.log(`worker.log:${msg}`);
  if (options.error) {
    // return callback(new Error("force error"));
    throw new Error(`forced error:${msg}`);
  }
  return callback(null, { message: `complete:${msg}` });
};

Worker.prototype.logTest.metadata = {
  options: { message: {}, error: {} },
};

Worker.prototype.testRequired = function testRequired(options, callback) {
  if (!options.foo) return callback('foo is required');

  const obj = JSON.parse(JSON.stringify(options));
  obj.last_run = new Date();
  // Set progress
  const seconds = options.seconds || 1;

  setTimeout(() => { this.progress({ message: 'Working.....', percent_complete: 10 }); }, seconds * 100);
  setTimeout(() => { this.progress({ message: 'Working.....', percent_complete: 30 }); }, seconds * 300);
  setTimeout(() => { this.progress({ message: 'Working.....', percent_complete: 50 }); }, seconds * 500);
  setTimeout(() => { this.progress({ message: 'Working.....', percent_complete: 70 }); }, seconds * 700);
  setTimeout(() => { this.progress({ message: 'Working.....', percent_complete: 90 }); }, seconds * 900);
  setTimeout(() => { callback(null, obj); }, seconds * 1000);
  return null;
};

Worker.prototype.testRequired.metadata = {
  options: {
    foo: { required: true },
    bar: { required: true },
  },
};

Worker.prototype.delayedEcho = function delayedEcho(_options, callback) {
  const options = JSON.parse(JSON.stringify(_options));
  debug(`\n\n\n\n***********\n\n\nDelayed Echo starting with options ${JSON.stringify(options)}\n\n********\n\n`);
  const optionHistory = {};
  Object.keys(options).forEach((i) => {
    if (i === 'option_history') return;
    optionHistory[i] = options[i];
  });
  options.option_history = (options.option_history || []).concat(JSON.stringify(optionHistory));

  if (options.counter === 0) {
    options.complete = true;
    return callback(null, options);
  }
  if (parseInt(options.counter, 10) !== options.counter) options.counter = 5;
  this.progress(`Running ${options.counter} more times`);
  options.counter -= 1;
  // Set a new poll date in one minute
  const a = new Date(new Date().getTime() + (1000));
  const newOpts = { status: 'pending', start_after_timestamp: a, options: { option_history: options.option_history, counter: options.counter } };
  return setTimeout(() => callback(null, null, newOpts), 100);
};

Worker.prototype.delayedEcho.metadata = {
  options: { counter: {} },
};

Worker.prototype.error = function error(options, callback) {
  const delay = parseInt(options.delay, 10) || 0;
  debug(`Delaying ${delay} milliseconds, then intentionally erroring`);
  setTimeout(() => {
    // eslint-disable-next-line
    require('FOOBAR');
    return callback();
  }, delay);
};
Worker.prototype.error.metadata = {
  options: {
    delay: {},
  },
};

Worker.prototype.criticalError = function criticalError(options, callback) {
  const delay = parseInt(options.delay, 10) || 0;
  debug(`Delaying ${delay} milliseconds, then intentionally erroring`);
  setTimeout(() => callback({ level: 'critical', message: 'Sample critical error' }), delay);
};

Worker.prototype.criticalError.metadata = {
  options: {
    delay: {},
  },
};

Worker.prototype.authError = function authError(options, callback) {
  return callback({ level: 'authentication', message: 'Sample Auth error' });
};

Worker.prototype.authError.metadata = {
  options: {
    delay: {},
  },
};

Worker.prototype.forever = function forever() { // just run forever
  const worker = this;
  const start = new Date();
  function go() {
    worker.progress(`Forever running for ${new Date().getTime() - start.getTime()} millis`);
    setTimeout(go, 5000);
  }
  setTimeout(go, 5000);
};

Worker.prototype.forever.metadata = {};

Worker.prototype.terminalError = function terminalError(options) {
  const delay = parseInt(options.delay, 10) || 0;
  debug(`Delaying ${delay} milliseconds, then intentionally illegal reference`);
  setTimeout(() => {
    const a = {};
    // This line is SUPPOSED to be horrible
    // eslint-disable-next-line
    a.b.c;
  }, delay);
};

Worker.prototype.terminalError.metadata = {
  options: {
    delay: {},
  },
};

Worker.prototype.errorThenSuccess = function errorThenSuccess(options, callback) {
  const delay = parseInt(options.delay, 10) || 5000;
  // eslint-disable-next-line
	callback("Sample Error");

  debug(`Delaying ${delay} milliseconds, then completing`);
  setTimeout(() => callback(null, { finished: 1 }), delay);
};

Worker.prototype.errorThenSuccess.metadata = {
  options: {
    delay: {},
  },
};

Worker.prototype.modify = function modify(options, callback) {
  const delay = parseInt(options.delay, 10) || 0;
  const counter = (parseInt(options.counter, 10) || 0) + 1;

  debug(`Delaying ${delay} milliseconds, then modifying with counter ${counter}`);
  setTimeout(() => {
    const m = {
      status: 'pending',
      options: {
        counter,
      },
      progress: `Progress message ${counter}`,
      start_after_timestamp: relativeDate('+5s'),
    };

    return callback(null, null, m);
  }, delay);
};

Worker.prototype.modify.metadata = {
  options: {
    delay: {},
  },
};

Worker.prototype.env = function env(options, callback) {
  return callback(null, process.env);
};

Worker.prototype.env.metadata = {
  options: {},
};

module.exports = Worker;
