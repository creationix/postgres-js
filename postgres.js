
var DEBUG = 0;

include('util.js');
include('md5.js');

function add_int(message, num) {
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
	add_int(message, body.length + 4);
	add_string(message, body);
	
	return message;
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
		var message = ['D'.charCodeAt(0)];
		add_int(message, name.length + 6);
		message.push(type.charCodeAt(0));
		add_string(message, name);
		return message;
	},
	Execute: function (name, max_rows) {
		var message = ['E'.charCodeAt(0)];
		add_int(message, name.length + 9);
		add_string(message, name);
		add_int(message, max_rows);
		return message;
	},
	Flush: function () {
		var message = ['H'.charCodeAt(0)];
		add_int(message, 4);
		return message;
	},
	FunctionCall: function () {
		// TODO: implement
	},
	Parse: function (name, query, var_types) {
		var message = ['P'.charCodeAt(0)];
		add_int(message, 4 + name.length + query.length + 4 + var_types.length * 4);
		add_string(message, name);
		add_string(message, query);
		add_int16(message, var_types.length);
		var_types.each(function (var_type) {
		  add_int(message, var_type);
		});
		return message;
	},
	PasswordMessage: function (password) {
		var message = ['p'.charCodeAt(0)];
		add_int(message, password.length + 5);
		add_string(message, password);
		return message;
	},
	Query: function (query) {
		var message = ['Q'.charCodeAt(0)];
		add_int(message, query.length + 5);
		add_string(message, query);
		return message;
	},
	SSLRequest: function () {
		var message = [];
		add_int(message, 8); // Message length
		add_int(message, 80877103); 
		return message;
	},
	StartupMessage: function (options) {
		var message = [];
		var text = "";
		for (var k in options) {
			if (options.hasOwnProperty(k)) {
				text += k + String.fromCharCode(0) + options[k] + String.fromCharCode(0);
			}
		}
		add_int(message, text.length + 9); // Message length
		add_int(message, 196608); // Protocol version number
		add_string(message, text); // options
		return message;
	},
	Sync: function () {
		var message = ['S'.charCodeAt(0)];
		add_int(message, 4);
		return message;
	},
	Terminate: function () {
		var message = ['X'.charCodeAt(0)];
		add_int(message, 4);
		return message;
	}
};

function parse_int32(message) {
	return message.shift() * 0xffffff + message.shift() * 0xffff + message.shift() * 0xff + message.shift();
}

function parse_int16(message) {
	return message.shift() * 0xff + message.shift();
}

function parse_string(message) {
	var text = "";
	while (message[0] !== 0) {
	  var n = message.shift();
		if (n < 0) {
		  n += 256;
		}
		text += String.fromCharCode(n);
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
		var n = message.shift();
		if (n < 0) {
		  n += 256;
		}
		text += String.fromCharCode(n);
	}
	return text;
}

function parse_response(message) {
	var code = String.fromCharCode(message.shift());
	var len = parse_int32(message);
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
      row.push(parse_raw_string(message, size));
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
	    print("Sending " + type + ": ");
	    p(args);
	    if (DEBUG > 1) {
	      print("->");
	      p(message);
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
	  while (data.length > 0) {
	    if (DEBUG > 1) {
        print("<-");
    		p(data);
    	}
  		var command = parse_response(data);
	    if (command.type) {
  	    if (DEBUG > 0) {
		      print("Received " + command.type + ": ");
		      p(command.args);
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

function onLoad() {

  var db = new exports.Connection("databasename", "username", "password");
  db.query("SELECT id FROM users LIMIT 1", p);
  db.query("SELECT id FROM users where id > 2 LIMIT 1", p);
  db.query("SELECT id FROM users WHERE id > 5 LIMIT 1", p);
  setTimeout(function () {
    db.query("SELECT * FROM users", p);
    db.close();
  }, 20);
}

