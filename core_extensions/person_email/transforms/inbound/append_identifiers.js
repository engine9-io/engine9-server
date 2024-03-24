const { createHmac } = require('node:crypto');

module.exports = async function ({ batch }) {
  const ids = [];
  batch.forEach((e) => {
    e.identifiers = e.identifiers || [];
    if (e.email) {
      const hashable = e.email.trim().toLowerCase();
      const value = createHmac('sha256', '')// no secret for this use case -- it's basically a shared id hashing setup, so a secret will be exposed anyhow
        .update(hashable)
        .digest('hex');
      e.identifiers.push({
        path: 'person_email', type: 'email', value,
      });
    }
  });

  return ids;
};
