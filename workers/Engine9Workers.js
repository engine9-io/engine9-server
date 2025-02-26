function Worker() {
}

Worker.metadata = {
  alias: 'engine9',
};

Worker.EchoWorker = require('./EchoWorker');
Worker.FileWorker = require('./FileWorker');
Worker.InputWorker = require('./InputWorker');
Worker.PersonWorker = require('./PersonWorker');
Worker.SQLWorker = require('./SQLWorker');
Worker.LocalPluginPersonWorker = require('./LocalPluginPersonWorker');

module.exports = Worker;
