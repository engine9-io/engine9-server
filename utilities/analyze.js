const { Transform } = require('node:stream');

const { pipeline } = require('node:stream/promises');

module.exports = async function analyzeStream(options) {
  const {
    stream,
    // hints of types of columns, useful when dealing with streams from databases
    // columnsTypes = {},
  } = options;

  const analysis = {
    records: 0,
    columns: {},
  };
  const analyzeTransform = new Transform({
    objectMode: true,
    transform(d, enc, cb) {
      analysis.records += 1;
      if (!d) return cb();
      Object.entries(d).forEach(([key, value]) => {
        analysis.columns[key] = analysis.columns[key] || {
          type: undefined,
          empty: 0,
          counters: {},
        };
        const r = analysis.columns[key];
        if (!r.min || value < r.min) r.min = value;
        if (!r.max || value > r.max) r.max = value;
        const type = typeof value;
        if (type === 'null' || type === 'undefined') {
          r.empty += 1;
        } else if (value instanceof Date) {
          r.type = 'date';
        } else if (type === 'number') {
          if (r.type !== 'string') r.type = 'number';
        } else {
          r.type = 'string';
          r.counters[value] = (r.counters[value] || 0) + 1;
        }
      });
      return cb();
    },
  });
  await pipeline(
    stream,
    analyzeTransform,
  );
  return { analysis };
};
