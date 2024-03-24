const mysqlTypes = [
  {
    type: 'id', column_type: 'bigint', unsigned: true, nullable: false, auto_increment: true, knex_method: 'bigIncrements',
  },
  {
    // person id is a very specific foreign id
    // person_id type definition is ALWAYS the same, regardless if it's a primary key or not
    // that way we never have to deal with null cases, and different platforms can rely on it having
    // a non-null value
    // Even though there's situations where it's not set, that value is then 0
    type: 'person_id', column_type: 'bigint', unsigned: true, nullable: false, default_value: 0, knex_method: 'bigint',
  },
  {
    // foreign ids can be nullable
    type: 'foreign_id', column_type: 'bigint', unsigned: true, nullable: true, knex_method: 'bigint',
  },
  {
    type: 'string', column_type: 'varchar', length: 255, knex_method: 'string', knex_args: ((o) => ([o.length || 255])),
  },
  { type: 'int', column_type: 'int', knex_method: 'integer' },
  { type: 'bigint', column_type: 'bigint', knex_method: 'bigint' },
  /* decimal(19,2) is chosen for currency
  as 4 digit currencies are rare enough to warrant another type */
  {
    type: 'currency', column_type: 'decimal(19,2)', knex_method: 'decimal', knex_args: [19, 2],
  },
  {
    type: 'decimal', column_type: 'decimal(19,4)', knex_method: 'decimal', knex_args: (() => ([19, 4])),
  },
  { type: 'double', column_type: 'double', knex_method: 'double' },
  {
    type: 'boolean',
    column_type: 'tinyint(1)',
    // nullable: true, //don't set this, otherwise we can't look up type boolean
    //  Defaults to nullable anyhow
    knex_method: 'boolean',
  },
  {
    type: 'text', column_type: 'text', length: 65535, knex_method: 'text',
  },
  {
    type: 'date_created',
    column_type: 'timestamp',
    default_value: 'current_timestamp()',
    nullable: false,
    knex_method: 'timestamp',
    knex_default_raw: 'current_timestamp()',
  },
  {
    type: 'last_modified',
    column_type: 'timestamp',
    default_value: 'current_timestamp() on update current_timestamp()',
    nullable: false,
    knex_method: 'timestamp',
    knex_default_raw: 'current_timestamp() on update current_timestamp()',
  },
  {
    type: 'url',
    column_type: 'text',
    length: 65535,
    knex_method: 'text',
  },
  { type: 'date', column_type: 'date', knex_method: 'date' },
  { type: 'datetime', column_type: 'datetime', knex_method: 'datetime' },
  { type: 'time', column_type: 'time', knex_method: 'time' },
];
module.exports = {
  getType(type) {
    return mysqlTypes.find((t) => t.type === type);
  },
  mysql: {
    standardToDialect(o, defaultColumn) {
      const output = { ...defaultColumn };
      let standardType = o;
      if (typeof o === 'object') {
        standardType = o.type;
      }
      if (!standardType) throw new Error('You must specify a type for');
      const lookup = mysqlTypes.find((d) => d.type === standardType);
      if (lookup) {
        return Object.assign(output, lookup, { type: standardType });
      }
      throw new Error(`Invalid type:${standardType}`);
    },
    standardToKnex(col) { // return {method,args} for knex
      // The name of the knex methods is ... inconsistent
      const { type } = col;
      const typeDef = mysqlTypes.find((t) => t.type === type);
      let { nullable } = col;
      if (nullable === undefined) {
        nullable = typeDef.nullable;
      }
      if (nullable === undefined) {
        nullable = true;
      }

      return {
        method: typeDef.knex_method,
        args: typeof typeDef.knex_args === 'function' ? typeDef.knex_args(col) : (typeDef.knex_args || []),
        unsigned: typeDef.unsigned || false,
        nullable,
        defaultValue: col.default_value !== undefined ? col.default_value : typeDef.knex_default,
        // raw values should always be defined strings in this file, not a function
        defaultRaw: typeDef.knex_default_raw,
      };
    },
    dialectToStandard(o, defaultColumn) {
      const input = { ...defaultColumn, ...o };
      // test first for strings
      if (input.column_type.indexOf('varchar') === 0) {
        // this is a string, which can have all the variations
        input.column_type = 'varchar';
        if (input.default_value
          && typeof input.default_value === 'string'
          && input.default_value.match(/^'.*'$/)) {
          input.default_value = input.default_value.slice(1, -1);
        }
        return { ...mysqlTypes.find((t) => t.type === 'string'), ...input };
      }

      if (input.column_type.slice(-9).toLowerCase() === ' unsigned') {
        input.column_type = input.column_type.slice(0, -9);
        input.unsigned = true;
      }
      if (input.column_type.indexOf('bigint') === 0) {
        input.column_type = 'bigint';
      }
      if (input.column_type.indexOf('int') === 0) {
        input.column_type = 'int';
      }
      const log = [];
      const typeDef = mysqlTypes.find(
        (type) => {
          const unmatchedAttributes = Object.keys(type).map((attr) => {
            if (attr === 'type' || attr.indexOf('knex_') === 0) return false;// ignore these
            if (type[attr] === input[attr]) {
              return false;
            }
            return `${attr}:${type[attr]} !== ${input[attr]}`;
          }).filter(Boolean);
          if (unmatchedAttributes.length > 0) {
            log.push({ type: type.type, unmatchedAttributes });
            return false;
          }
          return true;
        },
      );
      if (!typeDef) {
        throw new Error(`Could not find type that matches ${JSON.stringify(input)} \n${log.map((s) => JSON.stringify(s)).join('\n')}`);
      }

      return Object.assign(input, typeDef);
    },
  },
};
