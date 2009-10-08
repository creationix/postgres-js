node.mixin(require("/utils.js"));
node.mixin(require('md5.js'));
var Postgres = require('postgres.js');

function onLoad() {
  var db = new Postgres.Connection("username", "database", "password");
  ARGV.each(function (sql) {
    db.query(sql, function (data) {
      p(data);    
    });
  });
  db.close();
}


