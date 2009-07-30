/*jslint eqeqeq: true, immed: true, newcap: true, nomen: true, onevar: true, plusplus: true, regexp: true, undef: true, white: true, indent: 2 */

// Converter between javascript values and "raw"
// streams which are really arrays of 8 bit integers.
// the add functions return this so they can be chained.
// Note that this works directly on the array object.

// Encode number as 32 bit 2s compliment
Array.prototype.add_int32 = function (number) {
  var unsigned = (number < 0) ? (number + 0x100000000) : number;
  this.push(Math.floor(unsigned / 0xffffff));
  unsigned &= 0xffffff;
  this.push(Math.floor(unsigned / 0xffff));
  unsigned &= 0xffff;
  this.push(Math.floor(unsigned / 0xff));
  unsigned &= 0xff;
  this.push(Math.floor(unsigned));
  return this;
};

// Encode number as 16 bit 2s compliment
Array.prototype.add_int16 = function (number) {
  var unsigned = (number < 0) ? (number + 0x10000) : number;
  this.push(Math.floor(unsigned / 0xff));
  unsigned &= 0xff;
  this.push(Math.floor(unsigned));
  return this;
};

// Encode string without null terminator
Array.prototype.add_raw_string = function (text) {
  for (var i = 0, l = text.length; i < l; i += 1) {
    this.push(text.charCodeAt(i));
  }
  return this;
};

// Encode text as null terminated string
Array.prototype.add_cstring = function (text) {
  this.add_raw_string(text).push(0);
  return this;
};

// Encode as a null terminated array of cstrings
Array.prototype.add_multi_cstring = function (fields) {
  for (var i = 0, l = fields.length; i < l; i += 1) {
    this.add_cstring(fields[i]);
  }
  this.push(0);
  return this;
};

// Convert 4 bytes to signed 32 bit integer
Array.prototype.parse_int32 = function () {
  var unsigned = this.shift() * 0x1000000 + this.shift() * 0x10000 + this.shift() * 0x100 + this.shift();
  return (unsigned & 0x80000000) ? (unsigned - 0x100000000) : unsigned;
};

// Convert 2 bytes to signed 16 bit integer
Array.prototype.parse_int16 = function () {
  var unsigned = this.shift() * 0x100 + this.shift();
  return (unsigned & 0x8000) ? (unsigned - 0x10000) : unsigned;
};

// Grab number of bytes as a string
Array.prototype.parse_raw_string = function (len) {
  var text = "";
  while (len > 0) {
    len -= 1;
    text += String.fromCharCode(this.shift());
  }
  return text;
};

// Grab a null terminated string from the this
Array.prototype.parse_cstring = function () {
  var text = "";
  while (this.length > 0 && this[0] !== 0) {
    text += String.fromCharCode(this.shift());
  }
  this.shift();
  return text;
};

// Grab a null terminated array of null terminated strings
Array.prototype.parse_multi_cstring = function () {
  var fields = [];
  while (this[0] !== 0) {
    fields.push(this.parse_cstring());
  }
  this.pop();
  return fields;
};

//// Example usage
//var portal = "some_name";
//var stream = ['E'.charCodeAt(0)].add_int32(4 + portal.length + 1 + 4).add_cstring(portal).add_int32(0);
//p(stream);
//// output [69,0,0,0,18,115,111,109,101,95,110,97,109,101,0,0,0,0,0]
