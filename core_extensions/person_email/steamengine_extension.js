module.exports = {
  name: 'Core Person Email',
  streams: {
    append_person_email: {
      batch_size: 500,
      env: {
        PERSON_EMAIL_TABLE: 'SQL.tables.person_email',
      },
    },
  },
};
