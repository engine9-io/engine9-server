module.exports = {
  name: 'Core Person Email',
  dependencies:{
    person_email:'>1.0.0'
  },
  transforms: {
    person_email_append: {
      batch_size: 500,
      env: {
        PERSON_EMAIL_TABLE: 'SQL.tables.person_email',
      },
    },
    person_email_identifiers: {
      type:"identifiers",
      batch_size: 500,
      query:{
        table:'person_email',
        fields:[
          person_id,
          email,
          type
        ]
      },
      env: {
      },
    },
  },
};
