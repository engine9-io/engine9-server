const defaultColumn = {
  name: '',
  column_type: '',
  length: null,
  nullable: true,
  column_default: null,
  auto_increment: false,
};

const mysqlTypes = {
  id: { column_type: 'bigint', auto_increment: true },
  string: { column_type: 'varchar(255)', length: 255 },
  person_id: { column_type: 'bigint', is_nullable: false },
  int: { column_type: 'int' },
  bigint: { column_type: 'bigint' },
  /* decimal(19,2) is chosen for currency
  as 4 digit currencies are rare enough to warrant another type */
  currency: { column_type: 'decimal(19,2)' },
  decimal: { column_type: 'decimal(19,4)' },
  double: { column_type: 'double' },
  text: { column_type: 'text' },
  date_created: {
    column_type: 'timestamp',
    column_default: 'CURRENT_TIMESTAMP',
    nullable: false,
  },
  last_modified: {
    column_type: 'timestamp',
    column_default: 'CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',
    nullable: false,
  },
  url: { column_type: 'text' },
  date: { column_type: 'date' },
  datetime: { column_type: 'datetime' },
  time: { column_type: 'time' },

};
module.exports = {
  mysql: {
    types: mysqlTypes,
    toColumn(o) {
      const output = { ...defaultColumn };
      let lookupType = o;
      if (typeof o === 'object') {
        lookupType = o.type;
      }
      if (!lookupType) throw new Error('You must specify a type for');
      const lookup = mysqlTypes[lookupType];
      if (lookup) {
        return Object.assign(output, lookup);
      }
      throw new Error(`Invalid type:${lookupType}`);
    },
  },
};
