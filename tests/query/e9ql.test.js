/* eslint-disable camelcase */
const {
  describe, it,
} = require('node:test');
const assert = require('node:assert');
const dayjs = require('dayjs');
const {
  parse, toSql, withAnalysis, getEvalFn,
} = require('../../workers/e9ql');

const date_expr = (node, internal) => {
  const { operator, left_side, interval } = node;
  let fn = 'date_add';
  if (operator === '-') fn = 'date_sub';
  const { value, unit } = interval;
  return `${fn}(${internal(left_side)}, interval ${internal(value)} ${unit})`;
};

function assertParse(x, expected) {
  it(`should parse \`${x}\``, () => {
    const parsed = parse(x);
    const back = toSql({
      fieldFn: ((f) => f),
      tableFn: ((f) => f),
      valueFn: ((f) => {
        // eslint-disable-next-line eqeqeq
        if (f == parseInt(f, 10)) return f;
        return `"${f}"`;
      }),
      functions: { concat: 1, sum: 1, count: 1 },
      date_expr,
    }, parsed);
    assert.equal(back, expected);
  });
}

function evaluationTests(e9ql, lists) {
  describe(`evaluating ${e9ql}`, () => {
    const evalFn = getEvalFn({ e9ql });
    lists.forEach(([v, ex]) => {
      it(`should evaluate ${JSON.stringify(v)} to ${ex}`, () => {
        const result = evalFn(v);
        assert.deepEqual(ex, result);
      });
    });
  });
}

function roundTrip(x, expected, evalTests) {
  it(`should parse \`${x}\``, () => {
    const parsed = parse(x);
    const back = toSql({
      fieldFn: ((f) => f),
      tableFn: ((f) => f),
      valueFn: ((f) => {
        // eslint-disable-next-line eqeqeq
        if (f == parseInt(f, 10)) return f;
        return `"${f}"`;
      }),
      functions: { concat: 1, sum: 1, count: 1 },
      date_expr,
    }, parsed);
    assert.equal(back, expected);
  });

  it(`should parse \`${x}\` to same structure`, () => {
    const parsed = parse(x);
    const back = toSql({
      fieldFn: ((f) => f),
      tableFn: ((f) => f),
      valueFn: ((f) => {
        // eslint-disable-next-line eqeqeq
        if (f == parseInt(f, 10)) return f;
        return `"${f}"`;
      }),
      functions: { concat: 1, sum: 1, count: 1 },
      date_expr,
    }, parsed);
    const backParsed = parse(back);
    assert.deepEqual(backParsed, parsed);
  });

  if (evalTests && evalTests.length) {
    describe(`evaluation tests for \`${x}\``, () => {
      evalTests.forEach(([v, ex]) => {
        it(`should evaluate ${JSON.stringify(v)} to ${ex}`, () => {
          const result = getEvalFn({ e9ql: x })(v);
          assert.deepEqual(ex, result);
        });
      });
    });
  }
}

evaluationTests('ifnull(x,y,z)', [
  [{ x: 1, y: 2, z: 3 }, 1],
  [{ x: null, y: 2, z: 3 }, 2],
  [{ x: null, y: null, z: 3 }, 3],
]);
evaluationTests('ifnull(x,1+y,2+z)', [
  [{ x: 1, y: 2, z: 3 }, 1],
  [{ x: null, y: 2, z: 3 }, 3],
  [{ x: null, y: null, z: 3 }, 5],
]);

describe('roundTrip toSql(parse(x))', () => {
  roundTrip('concat("test",x)', 'concat("test", x)');
  roundTrip('sum(total)/count(*)', '(sum(total) / count(*))');
  roundTrip('test1.x + test2.y * 2', '(test1.x + (test2.y * 2))');
  roundTrip('(test1.x + test2.y) * 2', '((test1.x + test2.y) * 2)');
  roundTrip('(test1.x + test2.y / 2) * 2', '((test1.x + (test2.y / 2)) * 2)');

  roundTrip('not 1', '(not 1)');
  roundTrip('x and y and not z', '((x and y) and (not z))');

  roundTrip('x between 1 and 2', '(x between 1 and 2)');
  roundTrip('x between 1+3 and 2/x', '(x between (1 + 3) and (2 / x))');

  roundTrip('1 in (x,y,4,5,x*3)', '(1 in (x, y, 4, 5, (x * 3)))');
  roundTrip('1 not in (x,y,4,5,x*3)', '(1 not in (x, y, 4, 5, (x * 3)))');
  roundTrip('concat("test",2)', 'concat("test", 2)');

  roundTrip('case when x=1 then "test" else "hello" end', '(case when (x = 1) then "test" else "hello" end)', [
    [{ x: 1 }, 'test'],
    [{ x: 2 }, 'hello'],
  ]);

  roundTrip('date_add("2019-01-01", interval 1 day)', 'date_add("2019-01-01", interval 1 day)', [
    [{}, dayjs('2019-01-02T05:00:00.000Z').toDate()],
  ]);
  roundTrip('date_add(x, interval y day)', 'date_add(x, interval y day)', [
    [{ x: '2019-01-02', y: 1 }, dayjs('2019-01-03T05:00:00.000Z').toDate()],
    [{ x: '2019-01-02', y: 2 }, dayjs('2019-01-04T05:00:00.000Z').toDate()],
  ]);
  roundTrip('1.0 + 0.3', '(1 + 0.3)', [
    [{}, 1.3],
  ]);

  roundTrip('cast(x as char)', 'cast(x as char)');
  roundTrip('cast(x as signed)', 'cast(x as signed)');

  // object literals
  assertParse('if(1=1,null,1)', 'case when (1 = 1) then null else 1 end');
  assertParse('if(1=1,true,1)', 'case when (1 = 1) then true else 1 end');
  assertParse('if(1=1,false,1)', 'case when (1 = 1) then false else 1 end');

  roundTrip('date_add(now(), interval 1 day)', 'date_add(now(), interval 1 day)');
  roundTrip('date_sub(now(), interval 1 day)', 'date_sub(now(), interval 1 day)');
  roundTrip('now() + interval 1 day', 'date_add(now(), interval 1 day)');
  roundTrip('now() - interval 1 day', 'date_sub(now(), interval 1 day)');
  roundTrip('now()', 'now()');
  roundTrip('getdate()', 'now()');
  roundTrip('distinct x', 'distinct x');

  roundTrip('x < y', '(x < y)', [
    [{ x: 1, y: 2 }, true],
    [{ x: 2, y: 2 }, false],
    [{ x: 3, y: 2 }, false],
    [{ x: null, y: 2 }, null],
  ]);

  roundTrip(
    `case when amount<=20 then '$0-$20'
when amount>20 and amount<=50  then '$20-$50'
when amount>50 and amount<=100  then '$50-$100'
when amount>100 and amount<=250  then '$100-$250'
when amount>250 and amount<=500  then '$250-$500'
when amount>500 and amount<=1000  then '$500-$1000'
when amount>1000 and amount<=5000  then '$1000-$5000'
when amount>5000 then '$5000+' end`,
    '(case when (amount <= 20) then "$0-$20" when ((amount > 20) and (amount <= 50)) then "$20-$50" when ((amount > 50) and (amount <= 100)) then "$50-$100" when ((amount > 100) and (amount <= 250)) then "$100-$250" when ((amount > 250) and (amount <= 500)) then "$250-$500" when ((amount > 500) and (amount <= 1000)) then "$500-$1000" when ((amount > 1000) and (amount <= 5000)) then "$1000-$5000" when (amount > 5000) then "$5000+"  end)',
  );

  roundTrip(
    'case x when 1 then "one" when 2 then "two" else "other" end',
    '(case x when 1 then "one" when 2 then "two" else "other" end)',
    [
      [{ x: 1 }, 'one'],
      [{ x: 2 }, 'two'],
      [{ x: 3 }, 'other'],
      [{ x: 4 }, 'other'],
    ],
  );

  assertParse('if(x,y,z)', 'case when x then y else z end', [
    [{ x: true, y: 3, z: 4 }, 3],
    [{ x: false, y: 3, z: 4 }, 4],
  ]);

  assertParse('if(y,1+1,x)', 'case when y then (1 + 1) else x end');
  roundTrip('"test" like "t%1"', '("test" like "t%1")');
  roundTrip('"test" not like "t%1"', '("test" not like "t%1")');
  assertParse('if(true, null, false)', 'case when true then null else false end');
  roundTrip('x is null', '(x is null)', [
    [{ x: null }, true],
    [{ x: 1 }, false],
  ]);
  roundTrip('x is not null', '(x is not null)', [
    [{ x: null }, false],
    [{ x: 1 }, true],
  ]);

  roundTrip('x + null', '(x + null)', [
    [{ x: 1 }, null],
  ]);
});

describe('ifnull', () => {
  assertParse('ifnull(x,y)', 'case when x is not null then x else y end');
  assertParse(
    'ifnull(x,y,z)',
    'case when x is not null then x '
    + 'when y is not null then y else z end',
  );
  assertParse(
    'ifnull(x,1+y,2+z)',
    'case when x is not null then x '
    + 'when (1 + y) is not null then (1 + y) else (2 + z) end',
  );
});

function testAnalysis(baseTable, e9ql, expected) {
  it(`should correctly analyze \`${e9ql}\``, () => {
    const result = withAnalysis({
      e9ql,
      baseTable,

      fieldFn: ((f) => f),
      tableFn: ((f) => f),
      valueFn: ((f) => {
        // eslint-disable-next-line eqeqeq
        if (f == parseInt(f, 10)) return f;
        return `"${f}"`;
      }),
      functions: { concat: 1, sum: 1, count: 1 },
      date_expr,
    });

    delete result.getEvalFn;

    assert.deepEqual(result, expected);
  });
}

describe('withAnalysis(x)', () => {
  testAnalysis('test', '1+1', {
    cleaned: '(1 + 1)',
    refsByTable: {},
  });
  testAnalysis('test', 'x+1', {
    cleaned: '(x + 1)',
    refsByTable: { test: { x: 1 } },
  });
  testAnalysis('test1', 'x+test2.y', {
    cleaned: '(x + test2.y)',
    refsByTable: { test1: { x: 1 }, test2: { y: 1 } },
  });
  testAnalysis('t', 'if(x,y+1,test.z)', {
    cleaned: 'case when x then (y + 1) else test.z end',
    refsByTable: { t: { x: 1, y: 1 }, test: { z: 1 } },
  });
});
