const {
  describe, it, after, before,
} = require('node:test');
const assert = require('node:assert');
const PersonWorker = require('../../workers/PersonWorker');
const SchemaWorker = require('../../workers/SchemaWorker');
const SQLWorker = require('../../workers/SQLWorker');

describe('Upsert ids', async () => {
  before(async () => {
    let tables=sqlWorker.tables();
    await Promise.all(tables.map(table=>sqlWorker.drop({table});
  });

});
