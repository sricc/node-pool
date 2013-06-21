var Pool = require('../');
var Db   = require('../models/Db');

Pool({
	name     : 'Pool',
    create   : function(callback) {
     	var client = new Db();
     	client.createConnection(callback);
    },
    remove : function(client, callback) { 
    	client.end(callback);
    },

    // Max number of connections in the pool
    max : 10,

    // optional. if you set this, make sure to drain() 
    min : 1,

    // specifies how long a resource can stay idle in pool before being removed
    idleTimeout : 3000,

    // if true, logs via app.debug 
	log : true, 
}, function(error, pool) {
	pool.acquire(function(error, client) {
		pool.release(client, function() {
			setTimeout(function() {
				pool.acquire(function(error, client) {
					setTimeout(function() {}, 100000);
				})
			}, 5000);
		});		
	});
});