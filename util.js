/*jslint bitwise: true, eqeqeq: true, immed: true, newcap: true, nomen: true, onevar: true, plusplus: true, regexp: true, undef: true, white: true, indent: 2 */

// Extend some base objects with much needed functionality
Array.prototype.each = function (fn) {
  for (var i = 0, l = this.length; i < l; i += 1) {
    if (this.hasOwnProperty(i)) {
      fn.call(this, this[i]);
    }
  }
};
Array.prototype.pack = function () {
  var string = "";
  this.each(function (v) {
    string += String.fromCharCode(v);
  });
  return string;
};
Array.prototype.each_with_index = function (fn) {
  for (var i = 0, l = this.length; i < l; i += 1) {
    if (this.hasOwnProperty(i)) {
      fn.call(this, i, this[i]);
    }
  }
};

Object.prototype.each = function (fn) {
  for (var i in this) {
    if (this.hasOwnProperty(i)) {
      fn.call(this, i, this[i]);
    }
  }
};

String.prototype.each = function (fn) {
  for (var i = 0, l = this.length; i < l; i += 1) {
    fn.call(this, this[i]);  
  }
};
String.prototype.each_byte = function (fn) {
  for (var i = 0, l = this.length; i < l; i += 1) {
    fn.call(this, this.charCodeAt(i));  
  }
};
String.prototype.unpack = function () {
  var list = [];
  this.each_byte(function (b) {
    list.push(b);
  });
  return list;
};
