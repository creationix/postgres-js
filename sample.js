var sys = require("sys");
var Postgres = require('./postgres')

var db = new Postgres.Connection("database", "username", "password");
process.ARGV.slice(2).forEach(function (sql) {
  sys.puts(sql)
  db.query(sql, function (data) {
    sys.p(data);    
  });
});
db.close();


