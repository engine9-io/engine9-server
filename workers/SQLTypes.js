const mysqlTypes = [
  {
    type: 'id', column_type: 'bigint', auto_increment: true, knex_method: 'bigIncrements',
  },
  {
    type: 'string', column_type: 'varchar(255)', length: 255, knex_method: 'string', knex_args: ((o) => ([o.length || 255])),
  },
  {
    type: 'person_id', column_type: 'bigint', is_nullable: false, knex_method: 'bigint',
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
  { type: 'text', column_type: 'text', knex_method: 'text' },
  {
    type: 'date_created',
    column_type: 'timestamp',
    column_default: 'CURRENT_TIMESTAMP',
    nullable: false,
    knex_method: 'timestamp',
    knex_default_raw: 'CURRENT_TIMESTAMP',
  },
  {
    type: 'last_modified',
    column_type: 'timestamp',
    column_default: 'CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',
    nullable: false,
    knex_method: 'timestamp',
    knex_default_raw: 'CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',

  },
  {
    type: 'url',
    column_type: 'text',
    knex_method: 'text',
  },
  { type: 'date', column_type: 'date', knex_method: 'date' },
  { type: 'datetime', column_type: 'datetime', knex_method: 'datetime' },
  { type: 'time', column_type: 'time', knex_method: 'time' },
];
module.exports = {
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

      return {
        method: typeDef.knex_method,
        args: typeof typeDef.knex_args === 'function' ? typeDef.knex_args(col) : (typeDef.knex_args || []),
        defaultValue: col.default_value !== undefined ? col.default_value : typeDef.knex_default,
        // raw values should always be defined strings in this file, not a function
        defaultRaw: typeDef.knex_default_raw,
      };
    },
    dialectToStandard(o, defaultColumn) {
      const input = { ...defaultColumn, ...o };
      // test first for strings
      if (input.column_type.indexOf('varchar') === 0) {
        // this is a string
        input.type = 'string';
        return input;
      }
      const typeDef = mysqlTypes.find(
        (type) => {
          const unmatchedAttributes = Object.keys(type).filter((attr) => {
            if (attr === 'type') return false;
            return type[attr] !== input[attr];
          });
          if (unmatchedAttributes.length > 0) {
            console.error(`Did not match ${type.type} because of attributes ${unmatchedAttributes.join()}`);
            return false;
          }
          return true;
        },
      );
      if (!typeDef) throw new Error(`Could not find type that matches ${JSON.stringify(o)}`);

      return Object.assign(input, typeDef);
    },
  },
};
