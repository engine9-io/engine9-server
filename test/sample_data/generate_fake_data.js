// eslint-disable-next-line import/no-extraneous-dependencies
const { fakerEN_US: faker } = require('@faker-js/faker');
const fs = require('node:fs');
const { stringify } = require('csv');
const debug = require('debug')('generate_fake_data');
const { Readable } = require('node:stream');

let personId = 100000000;
function createRandomUser() {
  const region = faker.location.state({ abbreviated: true });
  personId += Math.floor(Math.random() * 5);
  return {
    person_id: personId, // userId: faker.string.uuid(),
    // username: faker.internet.userName(),
    given_name: faker.person.firstName(),
    family_name: faker.person.lastName(),
    email: faker.internet.email(),
    phone: faker.phone.number(),
    mobile_phone: faker.phone.number(),
    birthdate: faker.date.birthdate(),
    street_1: faker.location.streetAddress(true),
    street_2: '',
    city: faker.location.city(),
    region,
    postal_code: faker.location.zipCode({ state: region }),
    country: faker.location.countryCode('alpha-2'),
    avatar: faker.image.avatar(),
  };
}

const count = 1000;
const userArray = faker.helpers.multiple(createRandomUser, {
  count,
});
const recurs = ['', '', '', '', '', '', '', '', '', 'daily', 'weekly', 'monthly', 'monthly', 'monthly', 'monthly', 'monthly', 'quarterly', 'annually', 'annually'];
const formNames = ['Q1 Advocacy Action', 'Q2 Petition', 'Whales are cool'];
const sourceCodes = ['ACQ_EM_2023_X_123', 'EM_2023_X_123_A', 'EM_2023_X_123_B'];
const transactionArray = [];
const actionArray = [];

userArray.forEach((user) => {
  if (Math.random() > 0.3) return null;
  const { email } = user;
  const transCount = Math.random() * 4;
  for (let i = 0; i < transCount; i += 1) {
    transactionArray.push({
      remote_id: faker.uuid,
      entry_date: faker.date.past().toISOString(),
      entry_type: 'TRANSACTION',
      remote_input_id: faker.uuid,
      email,
      amount: faker.finance.amount({ max: 200, min: 5 }),
      recurs: recurs[Math.floor(Math.random() * recurs.length)],
      source_code: sourceCodes[Math.floor(Math.random() * sourceCodes.length)],
    });
    const formId = Math.floor(Math.random() * formNames.length);
    actionArray.push({
      ts: faker.date.past().toISOString(),
      remote_id: faker.string.uuid(),
      entry_type: 'FORM_SUBMISSION',
      remote_input_id: `form_${formId}`,
      remote_input_name: formNames[formId],
      email,
      source_code: sourceCodes[Math.floor(Math.random() * sourceCodes.length)],
      action_target: faker.internet.email(),
      action_content: faker.lorem.lines(),
    });
  }
  return null;
});

debug(`Creating ${count} fake people`);
Readable.from(userArray)
  .pipe(stringify({ header: true }))
  .pipe(fs.createWriteStream(`${__dirname}/../people/${count}_fake_people.csv`));
debug(`Creating ${count} fake transactions`);
Readable.from(transactionArray)
  .pipe(stringify({ header: true }))
  .pipe(fs.createWriteStream(`${__dirname}/${count}_fake_transactions.csv`));
debug(`Creating ${count} fake actions`);
Readable.from(actionArray)
  .pipe(stringify({ header: true }))
  .pipe(fs.createWriteStream(`${__dirname}/../actions/${count}_fake_actions.csv`));
console.log(actionArray.slice(0, 10));
