var DEBUG = 0;

include('util.js');

function add_int32(message, num) {
	var part;
	part = Math.floor(num / 0xffffff);
	message.push(part);
	num = num & 0xffffff;
	part = Math.floor(num / 0xffff);
	message.push(part);
	num = num & 0xffff;
	part = Math.floor(num / 0xff);
	message.push(part);
	num = num & 0xff;
	part = Math.floor(num);
	message.push(part);
}

function add_int16(message, num) {
	var part;
	part = Math.floor(num / 0xff);
	message.push(part);
	num = num & 0xff;
	part = Math.floor(num);
	message.push(part);
}

function add_string(message, text) {
  text.each_byte(function (b) {
    message.push(b);
  });
	message.push(0);
}

function format_message(type, body) {
	var message = [type.charCodeAt(0)];
	add_int32(message, body.length + 4);
	add_string(message, body);
	
	return message;
}

// http://www.postgresql.org/docs/8.3/static/protocol-message-formats.html
var formatter = {
  AddHeader: function (message, code) {
    var stream = [];
    if (code) {
      stream.push(code.charCodeAt(0));
    }
    add_int32(stream, message.length + 4);
    return stream.concat(message);
  },
	CopyData: function () {
		// TODO: implement
	},
	CopyDone: function () {
		// TODO: implement
	},
	Describe: function (name, type) {
		var message = [];
		message.push(type.charCodeAt(0));
		add_string(message, name);
		return formatter.AddHeader(message, 'D');
	},
	Execute: function (name, max_rows) {
		var message = [];
		add_string(message, name);
		add_int32(message, max_rows);
		return formatter.AddHeader(message, 'E');
	},
	Flush: function () {
		return formatter.AddHeader([], 'H');
	},
	FunctionCall: function () {
		// TODO: implement
	},
	Parse: function (name, query, var_types) {
		var message = [];
		add_string(message, name);
		add_string(message, query);
		add_int16(message, var_types.length);
		var_types.each(function (var_type) {
		  add_int32(message, var_type);
		});
		return formatter.AddHeader(message, 'P');
	},
	PasswordMessage: function (password) {
		var message = [];
		add_string(message, password);
		return formatter.AddHeader(message, 'p');
	},
	Query: function (query) {
		var message = [];
		add_string(message, query);
		return formatter.AddHeader(message, 'Q');
	},
	SSLRequest: function () {
		var message = [];
		add_int32(message, 80877103); 
		return formatter.AddHeader(message);
	},
	StartupMessage: function (options) {
		var message = [];
		var text = "";
		for (var k in options) {
			if (options.hasOwnProperty(k)) {
				text += k + String.fromCharCode(0) + options[k] + String.fromCharCode(0);
			}
		}
		add_int32(message, 196608); // Protocol version number
		add_string(message, text); // options
		return formatter.AddHeader(message);
	},
	Sync: function () {
		return formatter.AddHeader([], 'S');
	},
	Terminate: function () {
		return formatter.AddHeader([], 'X');
	}
};

// Convert 4 bytes to signed 32 bit integer
function parse_int32(message) {
	var unsigned = message.shift() * 0x1000000 + message.shift() * 0x10000 + message.shift() * 0x100 + message.shift();
  return (unsigned & 0x80000000) ? (unsigned - 0x100000000) : unsigned;
}

// Convert 2 bytes to signed 16 bit integer
function parse_int16(message) {
  var unsigned = message.shift() * 0x100 + message.shift();
  return (unsigned & 0x8000) ? (unsigned - 0x10000) : unsigned;
}

function parse_string(message) {
	var text = "";
	while (message.length > 0 && message[0] !== 0) {
		text += String.fromCharCode(message.shift());
	}
	message.shift();
	return text;
}

function parse_multi_string(message) {
  var fields = [];
	while (message[0] !== 0) {
		fields.push(parse_string(message));
	}
	message.pop();
	return fields;
}

function parse_raw_string(message, len) {
	var text = "";
	while (len > 0) {
		len -= 1;
		text += String.fromCharCode(message.shift());
	}
	return text;
}

function parse_response(code, message) {
	var type;
	var args = [];
	switch (code) {
	case 'R':
		var n = parse_int32(message);
		switch (n) {
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
		  args = [parse_raw_string(message, 2)];
		  break;
		case 5:
			type = "AuthenticationMD5Password";
			args = [parse_raw_string(message, 4)];
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
		var fields = parse_multi_string(message);
		var props = {};
		fields.each(function (field) {
		  props[field[0]] = field.substr(1);
		});
		args = [props];
	  break;
	case 'S':
		type = "ParameterStatus";
		args = [parse_string(message), parse_string(message)];
	  break;
	case 'K':
	  type = "BackendKeyData";
	  args = [parse_int32(message), parse_int32(message)];
	  break;
	case 'Z':
	  type = "ReadyForQuery";
	  args = [parse_raw_string(message, 1)];
	  break;
	case 'T':
	  type = "RowDescription";
	  var num_fields = parse_int16(message);
	  var row = [];
	  for (var i = 0; i < num_fields; i += 1) {
	    row.push({
	      field: parse_string(message),
	      table_id: parse_int32(message),
	      column_id: parse_int16(message),
	      type_id: parse_int32(message),
	      type_size: parse_int16(message),
	      type_modifier: parse_int32(message),
	      format_code: parse_int16(message)
	    });
    }
    args = [row];
    break;
  case 'D':
    type = "DataRow";
    var row = [];
    var num_cols = parse_int16(message);
    for (var i = 0; i < num_cols; i += 1) {
      var size = parse_int32(message);
      if (size === -1) {
        row.push(null);
      } else {
        row.push(parse_raw_string(message, size));
      }
    }
    args = [row];
    break;
  case 'C':
    type = "CommandComplete";
    args = [parse_string(message)];
    break;
	}
	if (!type) {
    puts("Unknown response " + code);	
	}
  return {type: type, args: args};
}

exports.Connection = function (database, username, password) {

	var connection = node.tcp.createConnection(5432);
  var events = new node.EventEmitter();
	var next_id = 0;
	var statements = {};
	var query_queue = [];
	var row_description;
	var query_callback;
	var results;
	var readyState = false;
	var closeState = false;

  // Sends a message to the postgres server
  function sendMessage(type, args) {
	  var message = formatter[type].apply(this, args);
	  if (DEBUG > 0) {
	    node.debug("Sending " + type + ": " + JSON.stringify(args));
	    if (DEBUG > 2) {
	      node.debug("->" + JSON.stringify(message));
	    }
	  }
	  connection.send(message, "raw");
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
	    var len = parse_int32(data);
	    var message = data.splice(0, len - 4);
	    if (DEBUG > 1) {
        node.debug("message: " + code + " " + JSON.stringify(message));
    	}
  		var command = parse_response(code, message);
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
			puts("CONNECTION DIED WITH ERROR");
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
      puts(e.S + ": " + e.M);
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
  }
};


