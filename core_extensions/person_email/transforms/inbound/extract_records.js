module.exports = async function (batch) {
  const tables = {
    person_email: [],
  };
  batch.forEach((o) => {
    tables.person_email.push({
      person_id: o.person_id,
      email: o.email,
    });
  });

  return tables;
};
