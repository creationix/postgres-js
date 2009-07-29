function wordwrap(str) {
    var m = 80;
    var b = "\r\n";
    var c = false;
    var i, j, l, s, r;
    str += '';
    if (m < 1) {
        return str;
    }
    for (i = -1, l = (r = str.split(/\r\n|\n|\r/)).length; ++i < l; r[i] += s) {
        for(s = r[i], r[i] = ""; s.length > m; r[i] += s.slice(0, j) + ((s = s.slice(j)).length ? b : "")){
            j = c == 2 || (j = s.slice(0, m + 1).match(/\S*(\s)?$/))[1] ? m : j.input.length - j[0].length || c == 1 && m || j.input.length + (j = s.slice(m).match(/^\S*/)).input.length;
        }
    }
    return r.join("\n");
}


exports.sendmail = function (from, to, subject, body) {
  var connection = node.tcp.createConnection(25);
  connection.addListener("connect", function (socket) {
    connection.send("helo localhost\r\n");
    connection.send("mail from: " + from + "\r\n");
    connection.send("rcpt to: " + to + "\r\n");
    connection.send("data\r\n");
    connection.send("To: " + to + "\r\n");
    connection.send("Subject: " + subject + "\r\n");
    connection.send("Content-Type: text/html\r\n");
    connection.send(wordwrap(body) + "\r\n");
    connection.send(".\r\n");
    connection.send("quit\r\n");
    connection.close();
  });
};



