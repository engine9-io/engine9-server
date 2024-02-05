const defaultColumn = {
  name: '',
  data_type: '',
  default_value: null,
  generation_expression: null,
  max_length: null,
  //  numeric_precision: 10,
  // numeric_scale: 0,
  is_generated: false,
  is_nullable: true,
  is_unique: false,
  is_primary_key: false,
  has_auto_increment: false,
  foreign_key_column: null,
  foreign_key_table: null,
  comment: '',
};

const mysqlTypes = {
  id: { data_type: 'bigint', has_auto_increment: true, is_primary_key: true },
  string: { data_type: 'varchar', max_length: 255 },
};
module.exports = {
  mysql: {
    types: mysqlTypes,
    toColumn(o) {
      const output = { ...defaultColumn };
      if (typeof o === 'object') return Object.assign(output, o);
      const lookup = mysqlTypes[o];
      if (lookup) return Object.assign(output, lookup);
      return Object.assign(output, {
        data_type: o,
        is_nullable: true,
      });
    },
  },
};
