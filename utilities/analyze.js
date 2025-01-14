const { Transform } = require('node:stream');

const { pipeline } = require('node:stream/promises');
const { uuidRegex } = require('@engine9/packet-tools');

module.exports = async function analyzeStream(options) {
  const {
    stream,
    // hints of types of fields, useful when dealing with streams from databases
    fieldHints,
  } = options;

  const analysis = {
    records: 0,
    fields: {},
  };
  function isNumeric(str) {
    // eslint-disable-next-line no-restricted-globals
    return !isNaN(str) && !isNaN(parseFloat(str));
  }
  const dateMatcher = /^([+-]?\d{4}(?!\d{2}\b))((-?)((0[1-9]|1[0-2])(\3([12]\d|0[1-9]|3[01]))?|W([0-4]\d|5[0-2])(-?[1-7])?|(00[1-9]|0[1-9]\d|[12]\d{2}|3([0-5]\d|6[1-6])))([T\s]((([01]\d|2[0-3])((:?)[0-5]\d)?|24:?00)([.,]\d+(?!:))?)?(\17[0-5]\d([.,]\d+)?)?([zZ]|([+-])([01]\d|2[0-3]):?([0-5]\d)?)?)?)?$/;
  let hints = null;
  if (fieldHints?.length > 0) {
    hints = {};
    fieldHints.forEach((d) => { hints[d.name] = d; });
  }

  const analyzeTransform = new Transform({
    objectMode: true,
    transform(d, enc, cb) {
      analysis.records += 1;
      if (!d) return cb();
      Object.entries(d).forEach(([key, _value]) => {
        let value = _value;
        analysis.fields[key] = analysis.fields[key] || {
          name: key,
          type: undefined,
          empty: 0,
          counters: {},
          hint: hints?.[key],
        };
        const r = analysis.fields[key];
        const type = typeof value;
        const isNumber = isNumeric(value);
        if (isNumber) {
          value = parseFloat(value);
        }

        if (!r.min || value < r.min) r.min = value;
        if (!r.max || value > r.max) r.max = value;

        if (value === null || type === 'undefined') {
          r.empty += 1;
        } else if (r.type !== 'string') { // once we go string we can't go back
          if (value && (value instanceof Date || (type === 'string' && dateMatcher.test(value)))) {
            if (type === 'string') value = new Date(value);
            if (r.type !== 'datetime' && value.getTime() % 100000 === 0) {
              r.type = 'date';
            } else {
              r.type = 'datetime';
            }
          } else if (type === 'string' && uuidRegex.test(value)) {
            r.type = 'uuid';
          } else if (isNumber) {
            r.isNumber = true;
            if (r.type === 'decimal') {
              // r.type = 'decimal';
            } else if (r.type === 'double') { // if we already have a double, we can't downscale
              // r.type = 'double';
            } else if (r.hint?.type === 'decimal') {
              r.type = 'decimal';
            } else if (r.hint?.type === 'double') {
              r.type = 'double';
            } else if (Number.isInteger(value)) {
              r.type = 'int';
            } else {
              r.type = 'double';
            }
          } else {
            // something has broken a rule and decayed to a string
            r.type = 'string';
            if (!r.min_length || value.length < r.min_length) r.min_length = value.length;
            if (!r.max_length || value.length > r.max_length) r.max_length = value.length;
          }
        } else {
          r.type = 'string';
          if (!r.min_length || value.length < r.min_length) r.min_length = value.length;
          if (!r.max_length || value.length > r.max_length) r.max_length = value.length;
        }
        r.counters[value] = (r.counters[value] || 0) + 1;
      });
      return cb();
    },
  });
  await pipeline(
    stream,
    analyzeTransform,
  );
  analysis.fields = Object.entries(analysis.fields).map(([, o]) => {
    const entries = Object.entries(o.counters);
    o.distinct = entries.length;
    o.sample = entries.sort((a, b) => b[1] - a[1]).slice(0, 32).map((d) => d[0]);
    delete o.counters;
    return o;
  });
  analysis.fields.forEach((f) => {
    f.type = f.type || 'string';
  });
  return analysis;
};
