const util = require('node:util');
const fs = require('node:fs');

const debug = require('debug')('UIWorker');

const JSON5 = require('json5');// Useful for parsing extended JSON
const PluginBaseWorker = require('./PluginBaseWorker');
const { deepMerge } = require('../utilities');

const fsp = fs.promises;

function Worker(worker) {
  PluginBaseWorker.call(this, worker);
}

util.inherits(Worker, PluginBaseWorker);
Object.keys(PluginBaseWorker.prototype).forEach((k) => {
  Worker.prototype[k] = PluginBaseWorker.prototype[k];
});

const DEFAULT_UI = {
  menu: {
    home: {
      title: 'Dashboard',
      icon: 'dashboard',
      url: '/',
    },
  },
  routes: {
    '/': {
      layout: 'grid',
      components: {
        main: [
          {
            component: 'StatCard',
            properties:
            {
              table: 'person',
              columns: [
                // 'id',
                { eql: 'YEAR(modified_at)', alias: 'year_modified' },
                { eql: 'count(id)', alias: 'count' },
              ],
              conditions: [
                { eql: "YEAR(modified_at)>'2020-01-01'" },
                { eql: 'id in (1,2,3)' },
                { eql: 'id in (3)' },
              ],
              groupBy: [{ eql: "YEAR(modified_at)>'2020-01-01'" }],
            },
          },
        ],
      },
    },
    '/profile': {
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
  debug('Getting config for ', { accountId, userId });
  const { paths } = await this.getActivePluginPaths();
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

module.exports = Worker;
