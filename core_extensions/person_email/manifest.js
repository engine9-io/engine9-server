module.exports = {
  name: 'Core Person Email',
  dependencies: {
    '@engine9-io/engine9-interfaces/person': '>1.0.0',
    '@engine9-io/engine9-interfaces/person_email': '>1.0.0',
  },
  transforms: {
    outbound: {
      person_email_append: {
        batch_size: 500,
        env: {
          PERSON_EMAIL_TABLE: 'SQL.tables.person_email',
        },
      },
    },
    inbound: {
      extract_identifiers: {
        type: 'identifiers',
        batch_size: 500,
      },
      extract_records: {
        type: 'records',
        batch_size: 500,
      },
    },
  },
};
