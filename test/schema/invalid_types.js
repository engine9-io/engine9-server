/* This is to test badly defined types, so should NOT work */
module.exports = {
  name: 'test_types',
  tables: [
    {
      name: 'test_types',
      fields: {
        foo: 'foobar',
        smallint: 'smallint', // not currently supported
        tinyint: 'tinyint', // not currently supported
        string_32: { type: 'string', length: 32 }, // length should be used
      },
    },
  ],
};
