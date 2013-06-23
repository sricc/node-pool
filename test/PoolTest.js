var should    = require('should');
var Db        = require('../models/Db');
var Pool      = require('../lib/Pool');
var PoolError = require('../lib/PoolError');
var async     = require('async');

describe('Pool', function() {
  before(function(done) {
    Pool({
      name  : 'Test Pool',
      create  : function(callback) { 
        var db = new Db();
        db.createConnection(callback);
      },
      destroy  : function(client, callback) { 
        client.end(callback);
      },
      min     : 2,
      max     : 5,
      idleTimeout : 200,
      log     : false
    }, function(error, pool) {
      if (error) throw error;

      mainPool = pool;
      done();
    });
  }); 

  it('should reject with an error if create is not specified', function(done) {
    Pool({destroy: function(callback) { callback(null, true); }}, function(error, pool) {
      should.exist(error);
      error.should.be.instanceof(PoolError);
      error.message.should.equal("Must specify 'create' function in options");
      should.not.exist(pool);
      done();
    });
  });

  it('should reject with an error if destroy is not specified', function(done) {
    Pool({create: function(callback) { callback(null, true);  }}, function(error, pool) {
      should.exist(error);
      error.should.be.instanceof(PoolError);  
      error.message.should.equal("Must specify 'destroy' function in options");    
      should.not.exist(pool);
      done();
    });
  });

  it('should return a Pool object', function(done) {
    Pool({
      create: function(callback) { callback(null, true); },
      destroy: function(callback) { callback(null, true); }
    }, function(error, pool) { 
      should.not.exist(error);
      should.exist(pool);
      pool.should.instanceof(Object);
      should.exist(pool.acquire);
      should.exist(pool.release);
      done();
    });
  });

  it('should set the options correctly', function(done) {
    Pool({
      name:        'Test Pool',
      create:      function(callback) { callback(null, true); },
      destroy:      function(callback) { callback(null, true); },
      min:         2,
      max:         5,
      idleTimeout: 200
    }, function(error, pool) {
      should.not.exist(error);
      should.exist(pool);
      pool._name.should.equal('Test Pool');
      pool._min.should.equal(2);
      pool._max.should.equal(5);
      pool._idleTimeout.should.equal(200);
      done();
    });
  });

  it('should create minimum clients when min is set in options', function(done) {
    Pool({
      name:        'Test Pool',
      create:      function(callback) { callback(null, true); },
      destroy:      function(callback) { callback(null, true); },
      min:         2
    }, function(error, pool) { 
      should.not.exist(error);
      should.exist(pool);
      pool.availableCount().should.equal(2);
      done();
    });
  });

  it('should not create any clients if min is not set in options', function(done) {
    Pool({
      name  : 'Test Pool',
      create  : function(callback) { 
        return createClient();
      },
      destroy  : function(client) { 
        client.end();
      },
      log   : false
    }, function(error, pool) {
      should.not.exist(error);
      should.exist(pool);
      pool.availableCount().should.equal(0);
      done();
    });
  });

  it('should destroy any idle clients from pool', function(done) {
    var idlePool, client1, client2, client3;
    async.series({
      pool: function(callback) {
        Pool({
          name:   'Test Pool',
          create: function(callback) { 
            var db = new Db();
            db.createConnection(callback);
          },
          destroy  : function(client, callback) { 
            client.end(callback);
          },
          min:         2,
          idleTimeout: 200,
          log:         false
        }, callback);
      }
    }, 
    function(error, results) {
        var pool = results.pool;
        async.parallel({
          client1: function(callback) { pool.acquire(callback); },
          client2: function(callback) { pool.acquire(callback); },
          client3: function(callback) { pool.acquire(callback); },
          client4: function(callback) { pool.acquire(callback); },
        }, 
        function(error, results) {
          pool.release(results.client1);
          pool.release(results.client2);
          pool.release(results.client3);
          pool.release(results.client4);
          setTimeout(function(id) {
            pool.totalCount().should.equal(0);
            pool.availableCount().should.equal(0);
            pool.activeCount().should.equal(0);
            clearTimeout(id);
            done();
          }, 2000);
        });
      });
  });

  describe('drain()', function() {
    it('should drain all clients regardless of state', function(done) {
      async.parallel([
        function(callback) { mainPool.acquire(callback); },
        function(callback) { mainPool.acquire(callback); },
        function(callback) { mainPool.acquire(callback); },
        function(callback) { mainPool.acquire(callback); }
      ], function(error, results) {
        mainPool.totalCount().should.equal(4);
        mainPool.availableCount().should.equal(0);
        mainPool.activeCount().should.equal(4);
        mainPool.waitingCount().should.equal(0);
        mainPool.drain(function() {
          mainPool.totalCount().should.equal(0);
          mainPool.availableCount().should.equal(0);
          mainPool.activeCount().should.equal(0);
          mainPool.waitingCount().should.equal(0);
          done();
        });
      });
    });
  });

  describe('acquire()', function() {
    context('when clients are available', function() {
      it('should get a new client from pool', function(done) {
        async.series({
          drained: function(callback) { mainPool.drain(callback) },
          client:  function(callback) { mainPool.acquire(callback) },
        }, function(error, results) {
          should.not.exist(error);
          should.exist(results.client);
          mainPool.totalCount().should.equal(1);
          mainPool.availableCount().should.equal(0);
          mainPool.activeCount().should.equal(1);
          done();
        });
      });
    });

    context('when no clients are available', function() {
      it('should create a new client', function(done) {
        async.series({
          drained: function(callback) { mainPool.drain(callback) },
          client:  function(callback) { mainPool.acquire(callback) },
        }, function(error, results) {
          should.not.exist(error);
          should.exist(results.client);
          results.client.should.exist;
          mainPool.totalCount().should.equal(1);
          mainPool.availableCount().should.equal(0);
          mainPool.activeCount().should.equal(1);

          mainPool.acquire(function(error, client) {
            should.not.exist(error);
            should.exist(client);
            mainPool.totalCount().should.equal(2);
            mainPool.availableCount().should.equal(0);
            mainPool.activeCount().should.equal(2);
            done();
          });
        });
      });
    });

    it('should respect the max connections', function(done) {
      async.series({
        drained: function(callback) { mainPool.drain(callback) },
        client1: function(callback) { mainPool.acquire(callback) },
        client2: function(callback) { mainPool.acquire(callback) },
        client3: function(callback) { mainPool.acquire(callback) },
        client4: function(callback) { mainPool.acquire(callback) },
        client5: function(callback) { mainPool.acquire(callback) }
      }, function(error, results) {
        should.not.exist(error);
        should.exist(results);
        mainPool.totalCount().should.equal(5);
        mainPool.availableCount().should.equal(0);
        mainPool.activeCount().should.equal(5);

        mainPool.acquire(function(error, client) {
          should.not.exist(error);
          mainPool.totalCount().should.equal(5);
          mainPool.availableCount().should.equal(0);
          mainPool.activeCount().should.equal(5);
          mainPool.waitingCount().should.equal(0);  
          done();       
        });

        mainPool.totalCount().should.equal(5);
        mainPool.availableCount().should.equal(0);
        mainPool.activeCount().should.equal(5);
        mainPool.waitingCount().should.equal(1);
        mainPool.release(results.client1, function() {});
      });
    });
  });

  describe('release()', function() {
    it('should release the client back to the pool', function(done) {
      async.series({
        drained: function(callback) { mainPool.drain(callback) },
        client: function(callback) { mainPool.acquire(callback) }
      }, function(error, results) {
        should.not.exist(error);
        should.exist(results);
        should.exist(results.client);
        mainPool.totalCount().should.equal(1);
        mainPool.availableCount().should.equal(0);
        mainPool.activeCount().should.equal(1);
        
        mainPool.release(results.client);
        mainPool.totalCount().should.equal(1);
        mainPool.availableCount().should.equal(1);
        mainPool.activeCount().should.equal(0);
        done();
      });
    });
  });

  after(function(done) {
    done();
  });
});