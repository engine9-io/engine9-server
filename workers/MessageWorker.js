/* eslint-disable camelcase */
const util = require('node:util');
const { TIMELINE_ENTRY_TYPES } = require('@engine9/packet-tools');
const BaseWorker = require('./BaseWorker');
const SQLiteWorker = require('./sql/SQLiteWorker');

function Worker(worker) {
  BaseWorker.call(this, worker);
}

util.inherits(Worker, BaseWorker);

Worker.prototype.inputDBStats = async function ({ sqliteFile }) {
  const sqlWorker = new SQLiteWorker({ accountId: this.accountId, sqliteFile });
  const records = (await sqlWorker.query('select count(*) as records from timeline')).data?.[0]?.records;
  const types = (await sqlWorker.query(`
      select entry_type_id,count(*) as records,
      count(distinct person_id) as distinct_people
      from timeline group by entry_type_id`)).data?.map(({ entry_type_id, ...rest }) => ({
    entry_type: TIMELINE_ENTRY_TYPES[entry_type_id],
    ...rest,
  }));
  const typesByHour = (await sqlWorker.query(`
      select strftime('%Y-%m-%d %H:00:00', ts/1000, 'unixepoch') as date,
      entry_type_id,
      count(*) as records,count(distinct person_id) as distinct_people 
      from timeline group by 1,2 order by 1,2`)).data?.map(({ entry_type_id, ...rest }) => ({
    entry_type: TIMELINE_ENTRY_TYPES[entry_type_id],
    ...rest,
  }));
  const domainStats = (await sqlWorker.query(`
    select email_domain as email_domain,
    count(*) as records,
    count(distinct person_id) as distinct_people,
    sum(case when entry_type_id=${TIMELINE_ENTRY_TYPES.EMAIL_SEND} then 1 else 0 end) as sent,
    sum(case when entry_type_id=${TIMELINE_ENTRY_TYPES.EMAIL_OPEN} then 1 else 0 end) as opened,
    sum(case when entry_type_id=${TIMELINE_ENTRY_TYPES.EMAIL_CLICK} then 1 else 0 end) as clicked,
    count(distinct case when entry_type_id=${TIMELINE_ENTRY_TYPES.EMAIL_SEND} then person_id else null end) as distinct_sent,
    count(distinct case when entry_type_id=${TIMELINE_ENTRY_TYPES.EMAIL_OPEN} then person_id else null end) as distinct_opened,
    count(distinct case when entry_type_id=${TIMELINE_ENTRY_TYPES.EMAIL_CLICK} then person_id else null end) as distinct_clicked,
    count(distinct case when entry_type_id in (${TIMELINE_ENTRY_TYPES.EMAIL_OPEN},${TIMELINE_ENTRY_TYPES.EMAIL_CLICK}) then person_id else null end) as distinct_open_or_clicked
    from timeline group by 1 order by count(*) desc limit 5`)).data;
  return {
    records, types, typesByHour, domainStats,
  };
};
Worker.prototype.inputDBStats.metadata = {
  options: {},
};

module.exports = Worker;
