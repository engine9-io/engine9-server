const fs = require('node:fs');

const debug = require('debug')('UIWorker');

const JSON5 = require('json5');// Useful for parsing extended JSON

const { deepMerge } = require('../utilities');

const fsp = fs.promises;

function Worker() {
}

const DEFAULT_UI = {
  menu: {
    home: {
      title: 'Dashboard',
      icon: 'dashboard',
      url: '/',
    },
  },
  routes: {
    '': {
      layout: 'grid',
      components: {
        main: [
          {
            component: 'StatCard',
            properties:
            {
              table: 'person',
              columns: [
                { eql: 'YEAR(modified_at)', alias: 'year_modified' },
                { eql: 'count(id)', alias: 'count' },
              ],
              conditions: [
                { eql: "YEAR(modified_at)>'2020-01-01'" },
              ],
              groupBy: [{ eql: 'YEAR(modified_at)' }],
            },
          },
        ],
      },
    },
    profile: {
      layout: 'grid',
      components: {
        main: [
          {
            component: 'Profile',
          },
        ],
      },
    },
  },

};

Worker.prototype.compileConsoleConfig = async function (path) {
  let p = path;
  if (path.indexOf('engine9-interfaces/') === 0) p = `${__dirname}/../../${path}/ui.console.json5`;
  debug(`Compiling ${p} from directory ${process.cwd()}`);
  const string = await fsp.readFile(p);
  const config = JSON5.parse(string);
  return config;
};

Worker.prototype.getConsoleConfig = async function ({ accountId, userId }) {
  let paths = null;
  if (accountId === 'dev') {
    paths = [
      'engine9-interfaces/person',
      'engine9-interfaces/person_email',
      'engine9-interfaces/person_address',
      'engine9-interfaces/person_phone',
      'engine9-interfaces/segment',
      'engine9-interfaces/message',
      'engine9-interfaces/job',
      'engine9-interfaces/query',
      'engine9-interfaces/report',
    ];
  } else {
    // This is here so we don't have to load the whole stack for
    // development accounts
    // eslint-disable-next-line global-require
    const PluginBaseWorker = require('./PluginBaseWorker');
    const worker = new PluginBaseWorker(this);
    debug('Getting config for ', { accountId, userId });
    paths = (await worker.getActivePluginPaths()).paths;
  }

  const configurations = await Promise.all(paths.map((path) => this.compileConsoleConfig(path)));
  let config = JSON.parse(JSON.stringify(DEFAULT_UI));
  configurations.forEach((c, i) => {
    try {
      config = deepMerge(config, c);
    } catch (e) {
      throw new Error(`Error with ui configuration for ${paths[i]}: ${e.message}`);
    }
  });

  return config;
};

Worker.prototype.getConsoleConfig.metadata = {
  options: {
    accountId: {},
    userId: {},
  },
};
Worker.prototype.getConsoleConfigString = async function (options) {
  const config = await this.getConsoleConfig(options);
  return JSON5.stringify(config, null, 4);
};
Worker.prototype.getConsoleConfigString.metadata = Worker.prototype.getConsoleConfig.metadata;

module.exports = Worker;
