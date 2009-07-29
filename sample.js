include('md5.js');
var postgres = require('postgres.js');

function onLoad() {
  var db = new postgres.Connection("database", "username", "password");
  db.query("SELECT * FROM sometable", function (data) {
    p(data);    
  });
  db.close();
}


