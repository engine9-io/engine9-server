module.exports = async function (batch, { PERSON_EMAIL_TABLE }) {
  const emails = await PERSON_EMAIL_TABLE.select(['person_id', 'email', 'type']).whereIn('person_id', batch.map((d) => d.id));
  const map = {};
  emails.forEach((r) => {
    map[r.person_id] = (map[r.person_id] || []).concat({ email: r.email, type: r.type });
  });
  batch.forEach((r) => {
    r.emails = map[r.id] || [];
  });
  return batch;
};
