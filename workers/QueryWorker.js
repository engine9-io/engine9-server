const util = require('util');
// const debug = require('debug')('QueryWorker');
// const debugMore = require('debug')('debug:SQLWorker');

const JSON5 = require('json5');// Useful for parsing extended JSON

const SQLWorker = require('./SQLWorker');

require('dotenv').config({ path: '.env' });

function Worker(worker) {
  SQLWorker.call(this, worker);
  this.accountId = worker.accountId;
  if (!this.accountId) throw new Error('No accountId provided to QueryWorker constructor');
  if (worker.knex) {
    this.knex = worker.knex;
  } else {
    this.auth = {
      ...worker.auth,
    };
  }
}

util.inherits(Worker, SQLWorker);
Worker.metadata = {
  alias: 'query',
};

/*
build an array of rules
{"combinator":"and",
    "rules":[
      {"field":"given_name","operator":"=","valueSource":"value","value":"Bob"},
      {"field":"family_name","operator":"beginsWith","valueSource":"value","value":"J"},
      { "combinator":"and","not":false
        "rules":[
          {"field":"age","operator":"=","valueSource":"value","value":42}],
      }
    ]
  }
*/
/*
export const defaultOperators = [
  { name: `=`, value: `=`, label: `=` } as const,
  { name: '!=', value: '!=', label: '!=' } as const,
  { name: '<', value: '<', label: '<' } as const,
  { name: '>', value: '>', label: '>' } as const,
  { name: '<=', value: '<=', label: '<=' } as const,
  { name: '>=', value: '>=', label: '>=' } as const,
  { name: 'contains', value: 'contains', label: 'contains' } as const,
  { name: 'beginsWith', value: 'beginsWith', label: 'begins with' } as const,
  { name: 'endsWith', value: 'endsWith', label: 'ends with' } as const,
  { name: 'doesNotContain', value: 'doesNotContain', label: 'does not contain' } as const,
  { name: 'doesNotBeginWith', value: 'doesNotBeginWith', label: 'does not begin with' } as const,
  { name: 'doesNotEndWith', value: 'doesNotEndWith', label: 'does not end with' } as const,
  { name: 'null', value: 'null', label: 'is null' } as const,
  { name: 'notNull', value: 'notNull', label: 'is not null' } as const,
  { name: 'in', value: 'in', label: 'in' } as const,
  { name: 'notIn', value: 'notIn', label: 'not in' } as const,
  { name: 'between', value: 'between', label: 'between' } as const,
  { name: 'notBetween', value: 'notBetween', label: 'not between' } as const,
]
*/
Worker.prototype.getOperation = function getOperation(operator, source, valueParam) {
  const value = [].concat(valueParam);
  function n(v) {
    return typeof v === 'number' ? v : this.escapeValue(v);
  }
  switch (operator) {
    case '=': return `=${this.escapeValue(value[0])}`;
    case '!=': return `<>${this.escapeValue(value[0])}`;
    case '<': return `<${this.escapeValue(value[0])}`;
    case '>': return `>${this.escapeValue(value[0])}`;
    case '<=': return `<=${this.escapeValue(value[0])}`;
    case '>=': return `>=${this.escapeValue(value[0])}`;
    case 'contains': return ` LIKE ${this.escapeValue(`%${value[0]}%`)}`;
    case 'beginsWith': return ` LIKE ${this.escapeValue(`${value[0]}%`)}`;
    case 'endsWith': return ` LIKE ${this.escapeValue(`%${value[0]}`)}`;
    case 'doesNotContain': return ` NOT LIKE ${this.escapeValue(`%${value[0]}%`)}`;
    case 'doesNotBeginWith': return ` NOT LIKE ${this.escapeValue(`${value[0]}%`)}`;
    case 'doesNotEndWith': return ` NOT LIKE ${this.escapeValue(`%${value[0]}`)}`;
    case 'null': return ' IS NULL';
    case 'notNull': return ' IS NOT NULL';
    case 'in': return ` IN (${value.map(n).join(',')})`;
    case 'notIn': return ` NOT IN (${value.map(n).join(',')})`;
    case 'between': return ` BETWEEN ${n(value[0])} AND ${n(value[1])}`;
    case 'notBetween': return ` NOT BETWEEN ${n(value[0])} AND ${n(value[1])}`;
    default: throw new Error(`Unhandled operation type:${operator}`);
  }
};
Worker.prototype.buildRule = function (rule) {
  if (rule.rules) {
    return `${rule.not ? ' NOT ' : ''}(${rule.rules
      .map((r) => this.buildRule(r))
      .join(rule.combinator === 'or' ? ' OR ' : ' AND ')})`;
  }
  return this.escapeColumn(rule.field)
    + this.getOperation(rule.operator, rule.valueSource, rule.value);
};

Worker.prototype.build = async function (queryProp) {
  if (!queryProp) throw new Error('query is required');
  let query = queryProp;
  if (typeof query === 'string') {
    query = JSON5.parse(query);
  }
  return this.buildRule(query);
};

Worker.prototype.build.metadata = {
  options: {
    query: {},
  },
};
module.exports = Worker;
