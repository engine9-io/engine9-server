const repl = require('repl');
const util = require('node:util');
const { bool } = require('../../utilities');

function Worker() {}
/*
const desc = /^(?:desc|describe) ([a-z0-9_.]*)$/i;
const fields = /^(?:fields) ([a-z0-9_.]*)$/i;
const showTables = /show tables like '(.*)'$/i;
const showViews = /show views like '(.*)'$/i;
const showIndexes = /show indexes from ([a-z0-9_.]*)$/i;
const showCreateView = /show create view ([a-z0-9_.]*)$/i;
const showCreateTable = /show create table ([a-z0-9_.]*)$/i;
const showProcessList = /show processlist([ a-z0-9_.]*)$/i;
const showTransactions = /show transactions([ a-z0-9_.]*)$/i;
const getTableSizes = /(show|get) table sizes([ a-z0-9_.]*)$/i;
const dropTable = /drop table ([a-z0-9_.]*)$/i;
const killall = /killall([ a-z0-9_.]*)$/i;
const kill = /kill\s+([0-9]*)$/i;
const showStatus = /^show engine innodb status/i;
const week = /week\(([a-z0-9_.-]*)\)/ig;
const month = /month\(([a-z0-9_.-]*)\)/ig;
const year = /year\(([a-z0-9_.-]*)\)/ig;
*/

Worker.prototype.cli = async function (options) {
  const worker = this;
  const info = { user: '', database: '', ...worker.auth };

  const dumb = bool(options.dumb, false);

  /*
  let history = '';
  try {
    history = await fsp.readFile('.e9_cli_history');
  } catch (e) {
    debug('.e9_cli_history');
  }

   history = (history || '').split('\n');
  */

  let lastEnd = null;
  let lastCounter = null;
  function output(...args) {
    // eslint-disable-next-line
    console.log(...args);
  }

  return new Promise(() => {
    repl.start({
      prompt: (`${worker.accountId} ${info.user ? `${info.user}@` : ''}${info.database}> `),
      async eval(_cmd, context, filename, callback) {
        let cmd = _cmd;
        let json = false;

        const start = Date.now();
        cmd = cmd.trim();

        if (cmd === 'exit' || cmd === '.exit') {
          output('Bye!');
          process.exit(0);
        }
        let tableFormat = null;

        if (cmd.slice(-1) === ';') {
          cmd = cmd.slice(0, -1);
        } else if (cmd.slice(-2) === '\\G') {
          json = true; cmd = cmd.slice(0, -2);
        } else {
          return callback(new repl.Recoverable(''));
        }

        function raw(d) {
          const names = Object.keys(d[0]);
          output(names.join('\t'));
          d.forEach((data) => {
            output(names.map((n) => data[n]).join('\t'));
          });
          output(`\n${d.length} records`);
        }
        function cb(e, d, endFunc) {
          const end = Date.now();

          if (e) {
            output(cmd);
            let msg = e.message || e;
            if (msg.red)msg = msg.red;
            output(msg);
            cmd = '';
          } else if (!d) {
            output('No Results');
          } else if (Array.isArray(d)) {
            delete d.query; delete d.parameters;
            if (d.length === 0) {
              output('No results');
            } else if (json) {
              output(util.inspect(d, { colors: true, maxArrayLength: 1000 }));
            } else if (d.length <= 5 && !tableFormat) {
              const a = d.map((f) => ({ ...f }));
              output(util.inspect(a, { colors: true, maxArrayLength: 1000 }));
              // Some code to do some convenient counts by second
              const counter = parseInt(Object.values(d[0] || {})[0], 10);
              let s = '';
              if (counter && typeof counter === 'number' && lastCounter && lastEnd) {
                s = `, ~${(1000 * (counter - lastCounter)) / (end - lastEnd)} records per second`;
              }
              if (counter) lastCounter = counter;
              output(`\n${d.length} records${s}`);
            } else if (tableFormat === 'raw') {
              raw(d);
            } else if (tableFormat === 'table' || JSON.stringify(d[0]).length < 400) {
              delete d.records; // sometimes added in
              raw(d);
            } else {
              raw(d);
            }
          } else if (d.sql) {
            output(d.sql);
          } else {
            output(util.inspect(d, { colors: true, maxArrayLength: 1000 }));
          }
          output(`${end - start}ms`);
          lastEnd = end;
          if (typeof endFunc === 'function') endFunc();
          callback();
        }

        if (!dumb) {
          if (cmd === 'show databases') {
            tableFormat = 'raw';
            return worker.showDatabases({}, cb);
          }
          if (cmd === 'show triggers') return worker.showTriggers({}, cb);
          if (cmd.toLowerCase() === 'drop temp tables') return worker.dropTempTables({}, cb);
          if (cmd.toLowerCase() === 'drop old temp tables') return worker.dropOldTempTables({}, cb);

          if (cmd === 'show tables') {
            tableFormat = 'raw';
            const o = await worker.tables({});
            return cb(null, (o.tables || o).map((table) => ({ table })));
          }

          /*
          let m;
          if (m = cmd.match(showTables)) {
            tableFormat = 'raw';
            const filter = m[1].replace(/%/g, '.+');
            return worker.showTables({ filter }, (e, o) => {
              if (e) return cb(e);
              return cb(null, (o.tables || o).map((table) => ({ table })));
            });
          }

          if (m = cmd.match(showViews)) {
            const filter = m[1].replace(/%/g, '');
            return worker.showTables({ type: 'view', filter }, (e, o) => {
              if (e) return cb(e);
              return cb(null, (o.tables || o).map((table) => ({ table })));
            });
          }

          if (m = cmd.match(showStatus)) {
            tableFormat = true;
          }

          if (m = cmd.match(showIndexes)) {
            tableFormat = 'table';
            return worker.getIndexes({ table: m[1] }, cb);
          }
          if (m = cmd.match(showProcessList)) {
            tableFormat = true;
            return worker.showProcessList({ filter: m[1].trim() }, cb);
          }
          if (m = cmd.match(getTableSizes)) {
            tableFormat = true;
            return worker.getTableSizes({ filter: m[2].trim() },
            (e, { tables, size_in_MB } = {}) =>
              cb(e, tables, () => output(`Total:${size_in_MB}MB`)));
          }

          if (m = cmd.match(showTransactions)) {
            tableFormat = true;
            return worker.showTransactions({ filter: m[1].trim() }, cb);
          }
          if (m = cmd.match(dropTable)) {
            tableFormat = true;
            return worker.dropTable({ table: m[1].trim() }, cb);
          }
          if (m = cmd.match(kill)) {
            if (worker.account_id === 'system') {
              output(`System kill ${m[1]}`);
              return worker.runQuery(`call mysql.rds_kill(${m[1].trim()})`, cb);
            }
          }
          if (m = cmd.match(killall)) {
            return worker.killAll({ filter: m[1].trim() }, cb);
          }
          if (m = cmd.match(desc)) {
            return worker.describe({ table: m[1] }, (e, d) => {
              if (e) return cb(e);
              tableFormat = 'table';
              return cb(null, d.fields);
            });
          }
          if (m = cmd.match(fields)) {
            return worker.describe({ table: m[1] }, (e, d) => {
              if (e) return cb(e);
              tableFormat = 'raw';
              return cb(null, d.fields.map((d) => ({ name: d.name })));
            });
          }
          if (m = cmd.match(showCreateView)) {
            return worker.getCreateView({ table: m[1] }, (e, o) => {
              worker.tidy(o, (e, tidy) => {
                if (e) {
                  output('Failed to tidy view', e);
                  return cb(null, { sql: o.sql });
                }
                return cb(null, { sql: tidy.tidy });
              });
            });
          }
          if (m = cmd.match(showCreateTable)) {
            return worker.getNativeCreateTable({ table: m[1] }, cb);
          }
          cmd = cmd.replace(week, (x, p1) => worker.getWeekFunction(p1));
          cmd = cmd.replace(month, (x, p1) => worker.getMonthFunction(p1));
          cmd = cmd.replace(year, (x, p1) => worker.getYearFunction(p1));

          // a bit buggy, but need more details
          cmd = worker.appendLimit(cmd);
          */
        }
        const { data } = await worker.query({ sql: cmd, silent: true });
        return cb(null, data);
      },
    });
  });
};

Worker.prototype.cli.metadata = {};

module.exports = Worker;
