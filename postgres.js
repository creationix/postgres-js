/*jslint bitwise: true, eqeqeq: true, immed: true, newcap: true, nomen: true, onevar: false, plusplus: true, regexp: true, undef: true, white: true, indent: 2 */
/*globals include md5 node exports */

include('util.js');
include('bits.js');

var DEBUG = 0;

Array.prototype.add_header = function (code) {
  var stream = [];
  if (code) {
    stream.push(code.charCodeAt(0));
  }
  stream.add_int32(this.length + 4);
  return stream.concat(this);
};


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
    return [].add_cstring(password).add_header('p');
  },
  Query: function (query) {
    return [].add_cstring(query).add_header('Q');
  },
  SSLRequest: function () {
    return [].add_int32(0x4D2162F).add_header();
  },
  StartupMessage: function (options) {
    var stream = [].add_int32(0x30000); // Protocol version number 3
    options.each(function (key, value) {
      stream.add_cstring(key);
      stream.add_cstring(value);
    });
    stream.push(0);
    return stream.add_header();
  },
  Sync: function () {
    return [].add_header('S');
  },
  Terminate: function () {
    return [].add_header('X');
  }
};


function parse_response(code, stream) {
  var type, args;
  args = [];
  switch (code) {
  case 'R':
    switch (stream.parse_int32()) {
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
      args = [stream.parse_raw_string(2)];
      break;
    case 5:
      type = "AuthenticationMD5Password";
      args = [stream.parse_raw_string(4)];
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
    stream.parse_multi_cstring().each(function (field) {
      args[0][field[0]] = field.substr(1);
    });
    break;
  case 'S':
    type = "ParameterStatus";
    args = [stream.parse_cstring(), stream.parse_cstring()];
    break;
  case 'K':
    type = "BackendKeyData";
    args = [stream.parse_int32(), stream.parse_int32()];
    break;
  case 'Z':
    type = "ReadyForQuery";
    args = [stream.parse_raw_string(1)];
    break;
  case 'T':
    type = "RowDescription";
    var num_fields = stream.parse_int16();
    var row = [];
    for (var i = 0; i < num_fields; i += 1) {
      row.push({
        field: stream.parse_cstring(),
        table_id: stream.parse_int32(),
        column_id: stream.parse_int16(),
        type_id: stream.parse_int32(),
        type_size: stream.parse_int16(),
        type_modifier: stream.parse_int32(),
        format_code: stream.parse_int16()
      });
    }
    args = [row];
    break;
  case 'D':
    type = "DataRow";
    row = [];
    var num_cols = stream.parse_int16();
    for (i = 0; i < num_cols; i += 1) {
      var size = stream.parse_int32();
      if (size === -1) {
        row.push(null);
      } else {
        row.push(stream.parse_raw_string(size));
      }
    }
    args = [row];
    break;
  case 'C':
    type = "CommandComplete";
    args = [stream.parse_cstring()];
    break;
  }
  if (!type) {
    node.debug("Unknown response " + code);  
  }
  return {type: type, args: args};
}

exports.Connection = function (database, username, password) {

  var connection = node.tcp.createConnection(5432);
  var events = new node.EventEmitter();
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
      node.debug("Sending " + type + ": " + JSON.stringify(args));
      if (DEBUG > 2) {
        node.debug("->" + JSON.stringify(stream));
      }
    }
    connection.send(stream, "raw");
  }
  
  // Set up tcp client
  connection.setEncoding("raw");
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
      node.debug("<-" + JSON.stringify(data));
    }
  
    while (data.length > 0) {
      var code = String.fromCharCode(data.shift());
      var len = data.parse_int32();
      var stream = data.splice(0, len - 4);
      if (DEBUG > 1) {
        node.debug("stream: " + code + " " + JSON.stringify(stream));
      }
      var command = parse_response(code, stream);
      if (command.type) {
        if (DEBUG > 0) {
          node.debug("Received " + command.type + ": " + JSON.stringify(command.args));
        }
        events.emit(command.type, command.args);
      }
    }
  });
  connection.addListener("eof", function (data) {
    connection.close();
  });
  connection.addListener("disconnect", function (had_error) {
    if (had_error) {
      node.debug("CONNECTION DIED WITH ERROR");
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
      node.debug(e.S + ": " + e.M);
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
    data.each_with_index(function (i, cell) {
      var description = row_description[i];
      row[description.field] = cell;
    });
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


