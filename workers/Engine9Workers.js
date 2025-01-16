function Worker() {
}

Worker.metadata = {
  alias: 'engine9',
};

Worker.EchoWorker = require('./EchoWorker');
Worker.FileWorker = require('./FileWorker');

module.exports = Worker;
