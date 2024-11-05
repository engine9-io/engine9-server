const mysql = require('./sql/dialects/MySQL');

module.exports = {
  default: mysql,
  mysql,
};
