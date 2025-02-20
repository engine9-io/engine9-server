// eslint-disable-next-line import/no-extraneous-dependencies
const { fakerEN_US: faker } = require('@faker-js/faker');
const fs = require('node:fs');
const { stringify } = require('csv');
const debug = require('debug')('generate_sample_data');
const { Readable } = require('node:stream');
const { pipeline } = require('node:stream/promises');
const { getTempFilename } = require('@engine9/packet-tools');

let personId = 100000000;
function createRandomPerson() {
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

const count = 5;
const recurs = ['', '', '', '', '', '', '', '', '', 'daily', 'weekly', 'monthly', 'monthly', 'monthly', 'monthly', 'monthly', 'quarterly', 'annually', 'annually'];
const formNames = ['Q1 Advocacy Action', 'Q2 Petition', 'Whales are cool'];
const sourceCodes = ['ACQ_EM_2023_X_123', 'EM_2023_X_123_A', 'EM_2023_X_123_B'];
const actionTargets = ['Dog Catcher Smith', 'Senator Tim Who', 'Congressman Agi Wannabe'];
function pick(a) {
  return a[Math.floor(Math.random() * a.length)];
}
const postfix = '.csv';

async function createSamplePersonFile() {
  const userArray = faker.helpers.multiple(createRandomPerson, {
    count,
  });
  const filename = await getTempFilename({ postfix });
  debug('Saving filename:', filename);
  await pipeline(
    Readable.from(userArray),
    stringify({ header: true }),
    fs.createWriteStream(filename),
  );
  return filename;
}

async function createSampleTransactionFile() {
  const transactionArray = [];

  const userArray = faker.helpers.multiple(createRandomPerson, {
    count,
  });
  userArray.forEach((user) => {
    if (Math.random() > 0.3) return null;
    const { email } = user;
    const transCount = Math.random() * 4;
    for (let i = 0; i < transCount; i += 1) {
      transactionArray.push({
        remote_entry_uuid: faker.uuid,
        entry_date: faker.date.past().toISOString(),
        entry_type: 'TRANSACTION',
        remote_input_id: faker.uuid,
        email,
        amount: faker.finance.amount({ max: 200, min: 5 }),
        recurs: pick(recurs),
        source_code: pick(sourceCodes),
      });
    }
    return null;
  });
  const filename = await getTempFilename({ postfix });
  await pipeline(
    Readable.from(transactionArray),
    stringify({ header: true }),
    fs.createWriteStream(filename),
  );
  return filename;
}

async function createSampleActionFile(opts) {
  const actionArray = [];
  const { users = 10, ts } = opts || {};

  debug(`Creating ${users} users`);
  const userArray = faker.helpers.multiple(createRandomPerson, { count: users });
  debug(`Created ${users} users`);

  let userCounter = 0;
  // eslint-disable-next-line no-restricted-syntax
  for (const user of userArray) {
    // const user = createRandomPerson();
    userCounter += 1;
    if (userCounter % 5000 === 0) debug(`Created ${userCounter}/${users} random users`);

    const { email } = user;
    const transCount = Math.random() * 3;
    for (let i = 0; i < transCount; i += 1) {
      const formId = Math.floor(Math.random() * formNames.length);
      actionArray.push({
        remote_entry_uuid: faker.string.uuid(),
        ts: ts || faker.date.past().toISOString(),
        entry_type: 'FORM_SUBMIT',
        remote_input_id: `form_${formId}`,
        remote_input_name: formNames[formId],
        email,
        source_code: pick(sourceCodes),
        action_target: pick(actionTargets),
        action_content: faker.lorem.lines(),
        'Sample Uppercase Content': faker.lorem.lines(),
      });
    }
  }
  const filename = await getTempFilename({ postfix });
  await pipeline(
    Readable.from(actionArray),
    stringify({ header: true }),
    fs.createWriteStream(filename),
  );
  return filename;
}

if (require.main === module) {
  debug(`Creating ${count} fake people`);
  createSamplePersonFile();
}
module.exports = {
  createSamplePersonFile,
  createSampleTransactionFile,
  createSampleActionFile,
};
