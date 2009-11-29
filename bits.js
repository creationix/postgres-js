/*jslint eqeqeq: true, immed: true, newcap: true, nomen: true, onevar: true, plusplus: true, regexp: true, undef: true, white: true, indent: 2 */

// Converter between javascript values and "raw"
// streams which are encoded as javascript strings
// that only use the first 8 bits of each character.

// Quick alias to shorten call time and to shorten code
var chr = String.fromCharCode;

// Encode number as 32 bit 2s compliment
String.prototype.add_int32 = function (number) {
  var a, b, c, d, unsigned;
  unsigned = (number < 0) ? (number + 0x100000000) : number;
  a = Math.floor(unsigned / 0xffffff);
  unsigned &= 0xffffff;
  b = Math.floor(unsigned / 0xffff);
  unsigned &= 0xffff;
  c = Math.floor(unsigned / 0xff);
  unsigned &= 0xff;
  d = Math.floor(unsigned);
  return this + chr(a) + chr(b) + chr(c) + chr(d);
};

// Encode number as 16 bit 2s compliment
String.prototype.add_int16 = function (number) {
  var a, b, unsigned;
  unsigned = (number < 0) ? (number + 0x10000) : number;
  a = Math.floor(unsigned / 0xff);
  unsigned &= 0xff;
  b = Math.floor(unsigned);
  return this + chr(a) + chr(b);
};

// Encode string without null terminator
String.prototype.add_raw_string = function (text) {
  return this + text;
};

// Encode text as null terminated string
String.prototype.add_cstring = function (text) {
  return this + text + "\0";
};

// Encode as a null terminated array of cstrings
String.prototype.add_multi_cstring = function (fields) {
  return this + fields.join("\0") + "\0\0";
};

// Convert 4 characters to signed 32 bit integer
String.prototype.parse_int32 = function () {
  var unsigned = this.charCodeAt(0) * 0x1000000 + this.charCodeAt(1) * 0x10000 + this.charCodeAt(2) * 0x100 + this.charCodeAt(3);
  return [4, (unsigned & 0x80000000) ? (unsigned - 0x100000000) : unsigned];
};

// Convert 2 bytes to signed 16 bit integer
String.prototype.parse_int16 = function () {
  var unsigned = this.charCodeAt(0) * 0x100 + this.charCodeAt(1);
  return [2, (unsigned & 0x8000) ? (unsigned - 0x10000) : unsigned];
};

// Grab number of bytes as a string
String.prototype.parse_raw_string = function (len) {
  return [len, this.substr(0, len)];
};

// Grab a null terminated string from the this
String.prototype.parse_cstring = function () {
  var pos = this.indexOf("\0");
  return [pos + 1, this.substr(0, pos)];
};

// Grab a null terminated array of null terminated strings
String.prototype.parse_multi_cstring = function () {
  var pos = this.indexOf("\0\0");
  return [pos + 2, this.substr(0, pos).split("\0")];
};

// Takes any number of commands to tell how to parse the binary input.
String.prototype.parse = function () {
  var pos, self, data;
  pos = 0;
  self = this;
  data = Array.prototype.map.call(arguments, function (command) {
    var args, pair;
    if (typeof command === 'string') {
      args = [];
    } else {
      args = command.slice(1);
      command = command[0];
    }
    pair = String.prototype["parse_" + command].apply(self.substr(pos), args);
    pos += pair[0];
    return pair[1];
  });
  return [pos, data];
};

