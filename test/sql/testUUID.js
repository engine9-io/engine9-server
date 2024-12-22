const {
  describe, it,
} = require('node:test');
// const assert = require('node:assert');
const debug = require('debug')('test/sql/testUUID');
const { parse, v4: uuidv4 } = require('uuid');
const sqlite = require('better-sqlite3');

describe('Create database, insert rows, select rows', async () => {
  const file1 = '../uuidTest1.db';
  const file2 = '../uuidTest2.db';
  const file3 = '../uuidTest3.db';

  it('should create 2 DBs', async () => {
    const db1 = sqlite(file1);
    const db2 = sqlite(file2);
    const db3 = sqlite(file3);
    const ids = Array.from({ length: 1000 }, () => uuidv4());

    db1.prepare('create table timeline (id blob)').run();
    db2.prepare('create table timeline (id char(32))').run();
    db3.prepare('create table timeline (id text)').run();

    const insert = 'insert into timeline (id) values (?)';
    const stmt1 = db1.prepare(insert);
    const stmt2 = db2.prepare(insert);
    const stmt3 = db3.prepare(insert);

    // eslint-disable-next-line no-restricted-syntax
    for (const item of ids) {
      stmt1.run(parse(item));
      stmt2.run(item);
      stmt3.run(item);
    }

    debug({ file1, file2, file3 });
  });
});
