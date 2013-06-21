// Mock DB client
function Client() {
  this.connect = function(callback) {
    return callback(null, this);
  };

  this.end = function(callback) {
    return callback(null, true);
  };
}

module.exports = function() {
  this.createConnection = function(callback) {
    var client = new Client();
    client.connect(callback);
  };
}