module.exports = async function (batch) {
  batch.tables = { person_address: [], ...batch.tables };
  batch.forEach((o) => {
    batch.tables.person_address.push({
      person_id: o.person_id,
      email: o.email,
    });
  });

  return batch;
};
