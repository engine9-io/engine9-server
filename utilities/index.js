const dayjs = require('dayjs');
const customParseFormat = require('dayjs/plugin/customParseFormat');

dayjs.extend(customParseFormat);
const crypto = require('node:crypto');
const JSON5 = require('json5');

function parseJSON5(s) {
  if (s == null || s === undefined) return null;
  if (typeof s === 'object') return s;
  return JSON5.parse(s);
}

function relativeDate(s, _initialDate) {
  let initialDate = _initialDate;
  if (!s || s === 'none') return null;
  if (typeof s.getMonth === 'function') return s;
  // We actually want a double equals here to test strings as well
  // eslint-disable-next-line eqeqeq
  if (parseInt(s, 10) == s) {
    const r = new Date(parseInt(s, 10));
    if (r === 'Invalid Date') throw new Error(`Invalid integer date:${s}`);
    return r;
  }

  if (initialDate) {
    initialDate = new Date(initialDate);
  } else {
    initialDate = new Date();
  }

  let r = s.match(/^([+-]{1})([0-9]+)([YyMwdhms]{1})([.a-z]*)$/);

  if (r) {
    let period = null;
    switch (r[3]) {
      case 'Y':
      case 'y': period = 'years'; break;

      case 'M': period = 'months'; break;
      case 'w': period = 'weeks'; break;
      case 'd': period = 'days'; break;
      case 'h': period = 'hours'; break;
      case 'm': period = 'minutes'; break;
      case 's': period = 'seconds'; break;
      default: period = 'minutes'; break;
    }

    let d = dayjs(initialDate);

    if (r[1] === '+') {
      d = d.add(parseInt(r[2], 10), period);
    } else {
      d = d.subtract(parseInt(r[2], 10), period);
    }
    if (d.toDate() === 'Invalid Date') throw new Error(`Invalid date configuration:${r}`);
    if (r[4]) {
      const opts = r[4].split('.').filter(Boolean);
      if (opts[0] === 'start') d = d.startOf(opts[1] || 'day');
      else if (opts[0] === 'end') d = d.endOf(opts[1] || 'day');
      else throw new Error(`Invalid relative date,unknown options:${r[4]}`);
    }

    return d.toDate();
  }
  if (s === 'now') {
    r = dayjs(new Date()).toDate();
    return r;
  }
  r = dayjs(new Date(s)).toDate();
  if (r === 'Invalid Date') throw new Error(`Invalid Date: ${s}`);
  return r;
}

const dateRegex = [
  { regex: /^[0-9]{1,2}\/[0-9]{2}$/, clean: (s) => `01/${s}`, format: 'DD/MM/YY' },
  { regex: /^[0-9]{1,2}\/[0-9]{4}$/, clean: (s) => `01/${s}`, format: 'DD/MM/YYYY' },
  { regex: /^[0-9]{1,2}\/[0-9]{1,2}\/[0-9]{2}$/, format: 'MM/DD/YY' },
  { regex: /^[0-9]{1,2}\/[0-9]{1,2}\/[0-9]{4}$/, format: 'MM/DD/YYYY' },
  { regex: /^[0-9]{4}$/, clean: (s) => `01/01/${s}`, format: 'DD/MM/YYYY' },
  { regex: /^.*$/ }, // try the normal parser
];
// Smart date parsing based on a few key heuristics
// Returns a valid ISO date, or NULL -- if you want invalid dates, parse them yourself
function parseDate(d) {
  if (!d) return null;
  if (typeof d !== 'string') {
    const o = dayjs(d);
    if (Number.isNaN(o)) return null;
    return o.toISOString();
  }
  const matching = dateRegex.find((r) => d.match(r.regex));
  if (!matching) return null;// not a valid date

  const input = matching.clean ? matching.clean(d) : d;

  const o = dayjs(input, matching.format);
  if (!o.isValid()) return null;
  return o.toISOString();
}

function parseRegExp(o, opts) {
  if (o instanceof RegExp) return o;
  try {
    const tempObject = {};
    switch (typeof o) {
      case 'object':
        Object.keys(o).forEach((k) => {
          tempObject[k] = parseRegExp(o[k], k);
        });
        return tempObject;

      case 'string':
        if (o.indexOf('/') === 0 && o.lastIndexOf('/') > 0) {
          const r = o.slice(1, o.lastIndexOf('/'));
          const g = o.slice(o.lastIndexOf('/') + 1);
          const flags = (g + (opts || '')).split('').join('');
          const re = new RegExp(r, flags);
          return re;
        }
        return new RegExp(o, opts || 'i');

      default:
        return o;
    }
  } catch (e) {
    return o;
  }
}
function bool(x, _defaultVal) {
  const defaultVal = (_defaultVal === undefined) ? false : _defaultVal;
  if (x === undefined || x === null || x === '') return defaultVal;
  if (typeof x !== 'string') return !!x;
  if (x === '1') return true; // 0 will return false, but '1' is true
  const y = x.toLowerCase();
  return !!(y.indexOf('y') + 1) || !!(y.indexOf('t') + 1);
}
function toCharCodes(x) {
  if (!x) return [];
  return Array.from(x).filter(Boolean).map((d) => d.charCodeAt(0));
}

function getIntArray(s, nonZeroLength) {
  let a = s || [];
  if (typeof a === 'number') a = [a];

  if (typeof s === 'string') a = s.split(',');
  a = a.filter((x) => (parseInt(x, 10) === s)).map((x) => parseInt(x, 10));
  if (nonZeroLength && a.length === 0) a = [0];
  return a;
}

function getStringArray(s, nonZeroLength) {
  let a = s || [];
  if (typeof a === 'number') a = String(a);
  if (typeof a === 'string') a = [a];

  if (typeof s === 'string') a = s.split(',');
  a = a.map((x) => x.toString().trim()).filter(Boolean);
  if (nonZeroLength && a.length === 0) a = [0];
  return a;
}

/*
        generate a unique hexadecimal key
*/
function generateUniqueKey(_opts) {
  const opts = _opts || {};
  const method = opts.method || 'sha1';
  const encoding = opts.encoding || 'hex';
  const bytes = opts.bytes || 2048;
  return crypto.createHash(method).update(crypto.randomBytes(bytes)).digest(encoding);
}

/*
An error that can take an object as a constructor, that can be dereferenced later.
The object should have a 'message' property for the parent error.
*/
class ObjectError extends Error {
  constructor(data) {
    if (typeof data === 'string') {
      // normal behavior
      super(data);
    } else if (typeof data === 'object') {
      super(data.message);
      Object.keys(data).forEach((k) => {
        this[k] = data[k];
      });
      this.status = data.status;
    } else {
      super('(No error message)');
    }
  }
}

/**
* Deep merge two or more objects or arrays.
* (c) Chris Ferdinandi, MIT License, https://gomakethings.com
* Updated to keep a stack, which also limits it to only merging 2 objects
* This sacrifices multi-merge for ease of debugging complex hierarchies
 * @param   {*} a  The core object
 * @param   {*} b  The object to merge in
 * @returns {*}          The merged arrays or objects
 */
function deepMerge(a, b, _stack) {
  const objs = [a, b];
  const stack = _stack || [];
  /**
   * Get the object type
   * @param  {*}       obj The object
   * @return {String}      The object type
   */
  function getType(obj) {
    return Object.prototype.toString.call(obj).slice(8, -1).toLowerCase();
  }

  /**
   * Deep merge two objects
   * @return {Object}
   */
  function mergeObj(clone, obj) {
    // eslint-disable-next-line no-restricted-syntax
    for (const [key, value] of Object.entries(obj)) {
      stack.push(key);
      const type = getType(value);
      if (clone[key] !== undefined && ['array', 'object'].includes(type)) {
        const cloneType = getType(clone[key]);
        if (cloneType === type) {
          clone[key] = deepMerge(clone[key], value, stack);
        } else {
          throw new Error(`Deep Merge cowardly not merging key ${stack.join('.')}, which has unmatching types: ${cloneType}!=${type}`);
        }
      } else {
        clone[key] = structuredClone(value);
      }
    }
  }

  // Create a clone of the first item in the objs array
  let clone = structuredClone(objs.shift());

  // Loop through each item
  // eslint-disable-next-line no-restricted-syntax
  for (const obj of objs) {
    // Get the object type
    const type = getType(obj);

    // If the current item isn't the same type as the clone, replace it
    if (getType(clone) !== type) {
      clone = structuredClone(obj);
      // eslint-disable-next-line no-continue
      continue;
    }

    // Otherwise, merge
    if (type === 'array') {
      clone = [...clone, ...structuredClone(obj)];
    } else if (type === 'object') {
      mergeObj(clone, obj);
    } else {
      clone = obj;
    }
  }

  return clone;
}

module.exports = {
  bool,
  parseRegExp,
  parseJSON5,
  parseDate,
  relativeDate,
  toCharCodes,
  getIntArray,
  getStringArray,
  generateUniqueKey,
  ObjectError,
  deepMerge,
};
