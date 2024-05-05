const util = require('node:util');
const fs = require('node:fs');

const debug = require('debug')('UIWorker');

const JSON5 = require('json5');// Useful for parsing extended JSON
const BaseWorker = require('./BaseWorker');
const { deepMerge } = require('../utilities');

const fsp = fs.promises;

function Worker(worker) {
  BaseWorker.call(this, worker);
}

util.inherits(Worker, BaseWorker);

const DEFAULT_UI = {
  menu: {
    home: {
      title: 'Dashboard',
      icon: 'dashboard',
      url: '/',
    },
    data: {
      title: 'Data',
      type: 'collapse',
      children: [
        {
          id: 'home-reports',
          title: 'Reports',
          icon: 'report',
          url: '/report',
        },
      ],
    },
  },
  routes: {
    '/': {
      layout: 'grid',
      components: {
        main: [
          { component: 'StatCard' },
          { component: 'StatCard' },
          { component: 'StatCard' },
          { component: 'StatCard' },
          { component: 'StatCard' },
          { component: 'StatCard' },
          { component: 'StatCard' },
        ],
      },
    },
  },
};

Worker.prototype.compileConsoleConfig = async function (path) {
  let p = path;
  if (path.indexOf('engine9-interfaces/') === 0) p = `../../${path}/ui.console.json5`;
  debug(`Compiling ${p} from directory ${process.cwd()}`);
  const string = await fsp.readFile(p);
  const config = JSON5.parse(string);
  return config;
};

Worker.prototype.getConsoleConfig = async function ({ accountId, userId }) {
  debug('Getting config for ', { accountId, userId });
  // this will be dynamic at some point
  const paths = [
    'engine9-interfaces/person',
    'engine9-interfaces/person_email',
    'engine9-interfaces/person_address',
    'engine9-interfaces/person_phone',
    'engine9-interfaces/segment',
    'engine9-interfaces/message',
  ];

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
