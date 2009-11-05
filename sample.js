process.mixin(require("sys"));
process.mixin(require('./md5'));
var Postgres = require('./postgres')

function onLoad() {
  var db = new Postgres.Connection("username", "database", "password");
  ARGV.each(function (sql) {
    db.query(sql, function (data) {
      p(data);    
    });
  });
  db.close();
}


