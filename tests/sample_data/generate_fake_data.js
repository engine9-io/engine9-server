const { fakerEN_US: faker } = require('@faker-js/faker');
const fs = require('node:fs');
const { stringify } = require('csv');
const { Readable } = require('node:stream');

function createRandomUser() {
  const region = faker.location.state({ abbreviated: true });
  return {
    // userId: faker.string.uuid(),
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

const count = 100000;
const userArray = faker.helpers.multiple(createRandomUser, {
  count,
});
const recurs = ['', '', '', '', '', '', '', '', '', 'daily', 'weekly', 'monthly', 'monthly', 'monthly', 'monthly', 'monthly', 'quarterly', 'annually', 'annually'];
const transactionArray = [];

userArray.forEach((user) => {
  if (Math.random() > 0.3) return null;
  const { email } = user;
  const transCount = Math.random() * 10;
  for (let i = 0; i < transCount; i += 1) {
    transactionArray.push({
      email,
      transaction_date: faker.date.past().toISOString(),
      amount: faker.finance.amount({ max: 200, min: 5 }),
      recurs: recurs[Math.floor(Math.random() * recurs.length)],
    });
  }
  return null;
});

Readable.from(userArray)
  .pipe(stringify({ header: true }))
  .pipe(fs.createWriteStream(`${__dirname}/${count}_fake_people.csv`));

Readable.from(transactionArray)
  .pipe(stringify({ header: true }))
  .pipe(fs.createWriteStream(`${__dirname}/${count}_fake_transactions.csv`));
