# PostgreSQL for Javascript

This library is a implementation of the PostgreSQL backend/frontend protocol in javascript.
It uses the node.js tcp and event libraries.  A javascript md5 library is included for servers that require md5 password hashing (this is default).

## Example use

    var sys = require("sys");
    var Postgres = require('postgres.js');

    function onLoad() {
      var db = new Postgres.Connection("database", "username", "password");
      db.query("SELECT * FROM sometable", function (data) {
        sys.p(data);    
      });
      db.close();
    }


