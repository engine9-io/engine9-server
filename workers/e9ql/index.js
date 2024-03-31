/* eslint-disable camelcase */
const nearley = require('nearley');
const dayjs = require('dayjs');
const debug = require('debug')('e9ql');
const grammar = require('./e9ql-parse');

function parse(_sql) {
  const sql = _sql.trim();
  const parser = new nearley.Parser(grammar.ParserRules, grammar.ParserStart);
  const parsed = parser.feed(sql);
  // console.log(JSON.stringify(parsed.results[0],null,2));
  if (parsed.length > 1) throw new Error('invalid - ambiguous');
  return parsed.results[0];
}

function walkNodes(options, parsed) {
  const { handlers } = options;
  if (!handlers) throw new Error('Handlers is required');
  function internal(node) {
    if (!node) return '';
    const { type } = node;
    const handler = handlers[type];
    if (!handler) {
      // eslint-disable-next-line no-console
      console.error('invalid node:', JSON.stringify(node, null, 2));
      throw new Error(`invalid type: ${type}`);
    }
    return handler(node, Object.assign(options, { internal }));
  }
  return internal(parsed);
}
const toSqlHandlers = {
  column: (node, { tableFn, columnFn, defaultTable }) => {
    const { table = defaultTable, name } = node;
    if (table) return `${tableFn(table)}.${columnFn(name)}`;
    return columnFn(name);
  },
  operator: (node, { internal }) => {
    const { left, right, operator } = node;
    const lExpr = internal(left);
    const rExpr = internal(right);
    return `(${lExpr} ${operator} ${rExpr})`;
  },
  not: (node, { internal }) => {
    const { operand } = node;
    const expr = internal(operand);
    return `(not ${expr})`;
  },
  decimal: (node) => parseFloat(node.value),
  string: (node, { valueFn }) => valueFn(node.string),
  identifier: (node, columnFn) => columnFn(node.value),
  distinct: ({ operand }, { internal }) => `distinct ${internal(operand)}`,
  ifnull: (node, { internal }) => {
    const { value, rest } = node;
    const list = [value].concat(rest.exprs || rest);
    const final = list[list.length - 1];
    const checks = list.slice(0, -1);
    return [
      'case',
      checks.map((x) => {
        const r = internal(x);
        return `when ${r} is not null then ${r}`;
      }).join(' '),
      `else ${internal(final)} end`,
    ].join(' ');
  },
  date_expr: (node, { internal, date_expr }) => {
    if (date_expr) return date_expr(node, internal);
    throw new Error('Failed to handle date_expr');
  },
  now: () => 'now()',
  function_call: (node, { internal, functions }) => {
    const { name: _name, parameters, select_all = false } = node;
    const name = (_name.value || '').toLowerCase();
    // if(!functions[name]) throw new Error('invalid function: '+name);

    let pExprs;
    if (!select_all) pExprs = parameters.map(internal);
    else pExprs = ['*'];

    if (typeof functions[name] !== 'function') return `${name}(${pExprs.join(', ')})`;
    return functions[name](pExprs);
  },
  between: (node, { internal }) => {
    const {
      value, lower, upper, not,
    } = node;

    const vExpr = internal(value);
    const lExpr = internal(lower);
    const uExpr = internal(upper);

    let notExpr = '';
    if (not) notExpr = 'not ';

    const base = `(${vExpr} ${notExpr}between ${lExpr} and ${uExpr})`;
    return base;
  },
  in: (node, { internal }) => {
    const { value, not, exprs } = node;

    const vExpr = internal(value);
    const eExprs = exprs.map(internal);

    let notExpr = '';
    if (not) notExpr = 'not ';

    return `(${vExpr} ${notExpr}in (${eExprs.join(', ')}))`;
  },
  case: (node, { internal }) => {
    const { match, when_statements, else: elseStatement } = node;

    const wExprs = when_statements.map(internal);
    const elseExpr = elseStatement && internal(elseStatement);

    let mExpr = ' ';
    if (match) mExpr = ` ${internal(match)} `;
    const out = `(case${mExpr}${wExprs.join(' ')} ${elseExpr !== undefined ? `else ${elseExpr}` : ''} end)`;
    debug('CASE statement resolved to:', out);
    return out;
  },
  when: (node, { internal }) => {
    const { condition, then } = node;

    const cExpr = internal(condition);
    const tExpr = internal(then);

    return `when ${cExpr} then ${tExpr}`;
  },
  if: (node, { internal }) => {
    const { condition, then, else: elseStatement } = node;

    const cExpr = internal(condition);
    const tExpr = internal(then);
    const eExpr = internal(elseStatement);

    // return `if(${cExpr}, ${tExpr}, ${eExpr})`;
    return `case when ${cExpr} then ${tExpr} else ${eExpr} end`;
  },
  like: (node, { internal }) => {
    const { not, value, comparison } = node;

    const vExpr = internal(value);
    const cExpr = internal(comparison);

    let notExpr = '';
    if (not) notExpr = 'not ';

    return `(${vExpr} ${notExpr}like ${cExpr})`;
  },
  true: () => 'true',
  false: () => 'false',
  null: () => 'null',
  is_null: (node, { internal }) => {
    const { value, not } = node;
    let notExpr = '';
    if (not) notExpr = 'not ';
    const vExpr = internal(value);
    return `(${vExpr} is ${notExpr}null)`;
  },
  cast: ({ value, data_type }, { internal }) => {
    const vExpr = internal(value);
    if (data_type.type !== 'data_type') throw new Error('Expected data_type');
    return `cast(${vExpr} as ${internal(data_type)})`;
  },
  data_type: ({ data_type }) => data_type,
};

const required = ['columnFn', 'valueFn', 'tableFn', 'functions', 'date_expr'];
function toSql(options, parsed) {
  const missing = required.filter((x) => !options[x]);
  if (missing.length) throw new Error(`${missing.join(', ')} are required parameters`);
  const {
    defaultTable, columnFn, tableFn, valueFn, functions,
  } = options;

  Object.keys(functions).forEach((fn) => {
    functions[fn.toLowerCase()] = functions[fn];
  });

  return walkNodes({
    handlers: toSqlHandlers,
    defaultTable,
    columnFn,
    tableFn,
    valueFn,
    functions,
    date_expr: options.date_expr,
  }, parsed);
}

const evalFnHandlers = {
  column: (node) => {
    const { name } = node;
    return (data) => data[name];
  },
  not: ({ operand }, internal) => {
    const operFn = internal(operand);
    return (data) => {
      const oper = operFn(data);
      if (oper == null) return null;
      if (typeof oper === 'string') return false;
      return !oper;
    };
  },
  operator: (node, { internal }) => {
    const { left, right, operator } = node;
    const leftFn = internal(left);
    const rightFn = internal(right);

    return (data) => {
      const l = leftFn(data);
      const r = rightFn(data);

      if (operator === 'or' || operator === 'and') {
        switch (operator) {
          case 'or': {
            if (l || r) return 1;
            if (l == null || r == null) return null;
            return false;
          }
          case 'and': {
            if (l === false || r === false) return false;
            if (l == null || r == null) return null;
            return true;
          }
          default:
        }
      }

      // using this override from sql for consistency
      if (l == null || r == null) return null;

      switch (operator) {
        case '<': return l < r;
        case '>': return l > r;
        case '=': return l === r;
        case '>=': return l >= r;
        case '<=': return l <= r;

        case '<>': return l !== r;
        case '!=': return l !== r;

        case '+': return l + r;
        case '-': return l - r;
        case '*': return l * r;
        case '/': return l / r;

        default: throw new Error(`Invalid operator: ${operator}`);
      }
    };
  },
  decimal: (node) => {
    const { value } = node;
    return () => parseFloat(value);
  },
  distinct: () => {
    throw new Error('distinct is unsupported on JS');
  },
  string: (node) => {
    const { string } = node;
    return () => (string || '').toString();
  },
  identifier: (node) => {
    const { value } = node;
    return (data) => data[value];
  },
  ifnull: (node, { internal }) => {
    const { value, rest } = node;
    const list = [value].concat(rest.exprs || rest);

    const listFns = list.map((x) => internal(x));
    return (data) => {
      let v;
      listFns.find((x) => {
        v = x(data);
        return v != null;
      });
      return v || null;
    };
  },
  date_expr: (node, { internal }) => {
    const { operator, left_side, interval } = node;
    const { value, unit } = interval;

    const leftFn = internal(left_side);
    const valueFn = internal(value);

    return (data) => {
      const left = leftFn(data);
      if (!left) return null;
      const date = dayjs(left);
      const v = valueFn(data);
      if (operator === '+') return date.add(v, unit).toDate();
      return date.subtract(v, unit).toDate();
    };
  },
  now: () => 'now()',
  function_call: () => { // (node, {internal,functions}) => {
    throw new Error('Invalid - have not implemented "function" calls for non-sql');
  },
  between: () => { // (node,{internal}) => {
    throw new Error('Invalid - have not implemented "between" for non-sqld');
  },
  in: () => { // (node,{internal}) => {
    throw new Error('Invalid - have not implemented "in" for non-sql');
  },
  case: (node, { internal }) => {
    const { match, when_statements, else: elseStatement } = node;
    const whenFns = when_statements.map(({ condition, then }) => ({
      condFn: internal(condition),
      thenFn: internal(then),
    }));
    const elseFn = internal(elseStatement);

    if (match) {
      const matchFn = internal(match);
      return (data) => {
        const mValue = matchFn(data);
        const c = whenFns.find((fn) => fn.condFn(data) === mValue);
        if (c) return c.thenFn(data);
        return elseFn(data);
      };
    }

    return (data) => {
      const c = whenFns.find((fn) => fn.condFn(data) === true);
      if (c) return c.thenFn(data);
      return elseFn(data);
    };
  },
  when: () => {
    throw new Error('Should have been handled by case');
  },
  if: (node, { internal }) => {
    const { condition, then, else: elseStatement } = node;

    const condFn = internal(condition);
    const thenFn = internal(then);
    const elseFn = internal(elseStatement);

    return (data) => {
      const check = condFn(data);
      if (check === true) return thenFn(data);
      return elseFn(data);
    };
  },
  like: () => { // (node,{internal}) => {
    throw new Error('Invalid - have not implemented like for non-sql');
  },
  true: () => () => true,
  false: () => () => false,
  null: () => () => null,
  is_null: (node, { internal }) => {
    const { value, not } = node;

    const valueFn = internal(value);
    if (not) return (data) => valueFn(data) != null;
    return (data) => valueFn(data) == null;
  },
};

function getEvalFn(options) {
  const { e9ql } = options;
  const parsed = parse(e9ql);

  return walkNodes({
    handlers: evalFnHandlers,
  }, parsed);
}

function withAnalysis(options) {
  const { e9ql, baseTable } = options;

  if (!baseTable) throw new Error('baseTable required');

  const parsed = parse(e9ql);
  if (!parsed) throw new Error('parsed is null from: ', e9ql);

  const cleaned = toSql(options, parsed);

  const refsByTable = {};
  function walk(node) {
    if (node.type === 'column') {
      const tableName = node.table || baseTable;
      refsByTable[tableName] = refsByTable[tableName] || {};
      const byTable = refsByTable[tableName];
      byTable[node.name] = 1;
    }
    Object.keys(node).forEach((k) => {
      if (Array.isArray(node[k])) node[k].forEach(walk);
      else if (node[k] && node[k].type) walk(node[k]);
    });
  }

  try {
    walk(parsed);
  } catch (e) {
    debug('parsed:', parsed);
    debug('Error walking parsed results:', e);
    throw new Error('Invalid SQL generated');
  }

  return { cleaned, refsByTable };
}

module.exports = {
  toSql, parse, withAnalysis, getEvalFn,
};
