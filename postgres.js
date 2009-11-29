/*jslint bitwise: true, eqeqeq: true, immed: true, newcap: true, nomen: true, onevar: true, plusplus: true, regexp: true, undef: true, white: true, indent: 2 */
/*globals include md5 node exports */

process.mixin(require('./bits'));
process.mixin(require('./md5'));
var tcp = require("tcp");
var sys = require("sys");

var DEBUG = 0;

String.prototype.add_header = function (code) {
  if (code === undefined) {
    code = "";
  }
  return code.add_int32(this.length + 4) + this;
};

Object.prototype.map_pairs = function (func) {
  var result = [];
  for (var k in this) {
    if (this.hasOwnProperty(k)) {
      result.push(func.call(this, k, this[k]));
    }
  }
  return result;
}

// http://www.postgresql.org/docs/8.3/static/protocol-message-formats.html
var formatter = {
  CopyData: function () {
    // TODO: implement
  },
  CopyDone: function () {
    // TODO: implement
  },
  Describe: function (name, type) {
    var stream = [type.charCodeAt(0)].add_cstring(name);
    return stream.add_header('D');
  },
  Execute: function (name, max_rows) {
    var stream = []
      .add_cstring(name)
      .add_int32(max_rows);
    return stream.add_header('E');
  },
  Flush: function () {
    return [].add_header('H');
  },
  FunctionCall: function () {
    // TODO: implement
  },
  Parse: function (name, query, var_types) {
    var stream = []
      .add_cstring(name)
      .add_cstring(query)
      .add_int16(var_types.length);
    var_types.each(function (var_type) {
      stream.add_int32(var_type);
    });
    return stream.add_header('P');
  },
  PasswordMessage: function (password) {
    return "".add_cstring(password).add_header('p');
  },
  Query: function (query) {
    return "".add_cstring(query).add_header('Q');
  },
  SSLRequest: function () {
    return "".add_int32(0x4D2162F).add_header();
  },
  StartupMessage: function (options) {
    // Protocol version number 3
    return ("".add_int32(0x30000) +
      options.map_pairs(function (key, value) {
        return "".add_cstring(key).add_cstring(value);
      }).join("") + "0").add_header();
  },
  Sync: function () {
    return [].add_header('S');
  },
  Terminate: function () {
    return [].add_header('X');
  }
};

// Parse response streams from the server
function parse_response(code, stream) {
  var type, args;
  args = [];
  switch (code) {
  case 'R':
    switch (stream.parse_int32()[1]) {
    case 0:
      type = "AuthenticationOk";
      break;
    case 2:
      type = "AuthenticationKerberosV5";
      break;
    case 3:
      type = "AuthenticationCleartextPassword";
      break;
    case 4:
      type = "AuthenticationCryptPassword";
      args = stream.substr(4).parse(["raw_string", 2])[1];
      break;
    case 5:
      type = "AuthenticationMD5Password";
      args = stream.substr(4).parse(["raw_string", 4])[1];
      break;
    case 6:
      type = "AuthenticationSCMCredential";
      break;
    case 7:
      type = "AuthenticationGSS";
      break;
    case 8:
      // TODO: add in AuthenticationGSSContinue
      type = "AuthenticationSSPI";
      break;
    }
    break;
  case 'E':
    type = "ErrorResponse";
    args = [{}];
    stream.parse("multi_cstring")[1][0].forEach(function (field) {
      args[0][field[0]] = field.substr(1);
    });
    break;
  case 'S':
    type = "ParameterStatus";
    args = stream.parse("cstring", "cstring")[1];
    break;
  case 'K':
    type = "BackendKeyData";
    args = stream.parse("int32", "int32")[1];
    break;
  case 'Z':
    type = "ReadyForQuery";
    args = stream.parse(["raw_string", 1])[1];
    break;
  case 'T':
    type = "RowDescription";
    var num_fields = stream.parse_int16()[1];
    stream = stream.substr(2);
    var row = [];
    for (var i = 0; i < num_fields; i += 1) {
      var parts = stream.parse("cstring", "int32", "int16", "int32", "int16", "int32", "int16");
      row.push({
        field: parts[1][0],
        table_id: parts[1][1],
        column_id: parts[1][2],
        type_id: parts[1][3],
        type_size: parts[1][4],
        type_modifier: parts[1][5],
        format_code: parts[1][6]
      });
      stream = stream.substr(parts[0]);
    }
    args = [row];
    break;
  case 'D':
    type = "DataRow";
    var data = [];
    var num_cols = stream.parse_int16()[1];
    stream = stream.substr(2);
    for (i = 0; i < num_cols; i += 1) {
      var size = stream.parse_int32()[1];
      stream = stream.substr(4);
      if (size === -1) {
        data.push(null);
      } else {
        data.push(stream.parse_raw_string(size)[1]);
        stream = stream.substr(size);
      }
    }
    args = [data];
    break;
  case 'C':
    type = "CommandComplete";
    args = stream.parse("cstring")[1];
    break;
  }
  if (!type) {
    sys.debug("Unknown response " + code);  
  }
  return {type: type, args: args};
}


exports.Connection = function (database, username, password, port) {
  
  // Default to port 5432
  if (port === undefined) {
    port = 5432;
  }

  var connection = tcp.createConnection(port);
  var events = new process.EventEmitter();
  var query_queue = [];
  var row_description;
  var query_callback;
  var results;
  var readyState = false;
  var closeState = false;

  // Sends a message to the postgres server
  function sendMessage(type, args) {
    var stream = formatter[type].apply(this, args);
    if (DEBUG > 0) {
      sys.debug("Sending " + type + ": " + JSON.stringify(args));
      if (DEBUG > 2) {
        sys.debug("->" + JSON.stringify(stream));
      }
    }
    connection.send(stream, "binary");
  }
  
  // Set up tcp client
  connection.setEncoding("binary");
  connection.addListener("connect", function () {
    sendMessage('StartupMessage', [{user: username, database: database}]);
  });
  connection.addListener("receive", function (data) {

    // Hack to work around bug in node
    // TODO: remove once Ry fixes bug
    for (var i = 0, l = data.length; i < l; i += 1) {
      if (data[i] < 0) {
        data[i] += 256;
      }
    }

    if (DEBUG > 2) {
      sys.debug("<-" + JSON.stringify(data));
    }
  
    while (data.length > 0) {
      var code = data[0];
      var len = data.substr(1, 4).parse_int32()[1];
      var stream = data.substr(5, len - 4);
      data = data.substr(len + 1);
      if (DEBUG > 1) {
        sys.debug("stream: " + code + " " + JSON.stringify(stream));
      }
      var command = parse_response(code, stream);
      if (command.type) {
        if (DEBUG > 0) {
          sys.debug("Received " + command.type + ": " + JSON.stringify(command.args));
        }
        command.args.unshift(command.type);
        events.emit.apply(events, command.args);
      }
    }
  });
  connection.addListener("eof", function (data) {
    connection.close();
  });
  connection.addListener("disconnect", function (had_error) {
    if (had_error) {
      sys.debug("CONNECTION DIED WITH ERROR");
    }
  });

  // Set up callbacks to automatically do the login
  events.addListener('AuthenticationMD5Password', function (salt) {
    var result = "md5" + md5(md5(password + username) + salt);
    sendMessage('PasswordMessage', [result]);
  });
  events.addListener('AuthenticationCleartextPassword', function () {
    sendMessage('PasswordMessage', [password]);
  });
  events.addListener('ErrorResponse', function (e) {
    if (e.S === 'FATAL') {
      sys.debug(e.S + ": " + e.M);
      connection.close();
    }
  });
  events.addListener('ReadyForQuery', function () {
    if (query_queue.length > 0) {
      var query = query_queue.shift();
      query_callback = query.callback;
      sendMessage('Query', [query.sql]);
      readyState = false;
    } else {
      if (closeState) {
        connection.close();
      } else {
        readyState = true;      
      }
    }
  });
  events.addListener("RowDescription", function (data) {
    row_description = data;
    results = [];
  });
  events.addListener("DataRow", function (data) {
    var row = {};
    for (var i = 0, l = data.length; i < l; i += 1) {
      var description = row_description[i];
      var value = data[i];
      if (value !== null) {
        // TODO: investigate to see if these numbers are stable across databases or
        // if we need to dynamically pull them from the pg_types table
        switch (description.type_id) {
        case 16: // bool
          value = value === 't';
          break;
        case 20: // int8
        case 21: // int2
        case 23: // int4
          value = parseInt(value, 10);
          break;
        }
      }
      row[description.field] = value;
    }
    results.push(row);
  });
  events.addListener('CommandComplete', function (data) {
    query_callback.call(this, results);
  });

  this.query = function (sql, callback) {
    query_queue.push({sql: sql, callback: callback});
    if (readyState) {
      events.emit('ReadyForQuery');
    }
  };
  this.close = function () {
    closeState = true;
  };
};


