const mysqlTypes = [
  { type: 'id', column_type: 'bigint', auto_increment: true },
  { type: 'string', column_type: 'varchar(255)', length: 255 },
  { type: 'person_id', column_type: 'bigint', is_nullable: false },
  { type: 'int', column_type: 'int' },
  { type: 'bigint', column_type: 'bigint' },
  /* decimal(19,2) is chosen for currency
  as 4 digit currencies are rare enough to warrant another type */
  { type: 'currency', column_type: 'decimal(19,2)' },
  { type: 'decimal', column_type: 'decimal(19,4)' },
  { type: 'double', column_type: 'double' },
  { type: 'text', column_type: 'text' },
  {
    type: 'date_created',
    column_type: 'timestamp',
    column_default: 'CURRENT_TIMESTAMP',
    nullable: false,
  },
  {
    type: 'last_modified',
    column_type: 'timestamp',
    column_default: 'CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',
    nullable: false,
  },
  { type: 'url', column_type: 'text' },
  { type: 'date', column_type: 'date' },
  { type: 'datetime', column_type: 'datetime' },
  { type: 'time', column_type: 'time' },
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
    dialectToStandard(o, defaultColumn) {
      const output = { ...defaultColumn, ...o };
      const typeDef = mysqlTypes.find(
        (type) => Object.keys(type).every((k) => {
          if (k === 'name') return true;
          return type[k] === output.k;
        }),
      );
      if (!typeDef) throw new Error(`Could not find type that matches ${JSON.stringify(o)}`);

      return Object.assign(output, typeDef);
    },
  },
};
