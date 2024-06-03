/* eslint-disable camelcase */
/*
  The PacketWorker is in charge of reading, pulling, and pushing packets from the
  core repository
*/

const util = require('node:util');
const PacketTools = require('engine9-packet-tools');
const BaseWorker = require('./BaseWorker');

function Worker(worker) {
  BaseWorker.call(this, worker);
}

util.inherits(Worker, BaseWorker);

// List contents of a remote packet
Worker.prototype.create = async function (options) {
  options.accountId = this.accountId;
  return PacketTools.create(options);
};
Worker.prototype.create.metadata = {
  options: {
    accountId: {},
    pluginId: {},
    target: { description: 'Target filename' },
    personFiles: {},
    timelineFiles: {},
    messageFiles: {},
    statisticsFiles: {},
  },
};
// List contents of a remote packet
Worker.prototype.manifest = async function (options) {
  return PacketTools.getManifest(options);
};
Worker.prototype.manifest.metadata = {
  options: {
    path: {},
  },
};

module.exports = Worker;
