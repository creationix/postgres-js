# PostgreSQL for Javascript

This library is a implementation of the PostgreSQL backend/frontend protocol in javascript.
It uses the node.js tcp and event libraries.  A javascript md5 library is included for servers that require md5 password hashing.

## Example use

    include('md5.js');
    var postgres = require('postgres.js');

    function onLoad() {
      var db = new postgres.Connection("database", "username", "password");
      db.query("SELECT * FROM sometable", function (data) {
        p(data);    
      });
      db.close();
    }


