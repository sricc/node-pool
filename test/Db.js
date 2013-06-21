
function Client() {
  this.connect = function(callback) {
    return callback(null, true);
  };

  this.end = function(callback) {
    return callback(null, true);
  };
}

module.exports = function() {
  this.createConnection = function() {
    return new Client();
  };
}