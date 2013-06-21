var db     = require('./Db');
var should = require('should');

var createClient = function() {
  var client = db.createConnection();

  client.connect();

  return Q.resolve(client);
}

describe('Pool', function() {
  before(function(done) {
    Pool({
      name  : 'Test Pool',
      create  : function() { 
        return createClient();
      },
      remove  : function(client) { 
        client.end();
      },
      min     : 2,
      max     : 5,
      idleTimeout : 200,
      log     : false
    }).then(function(pool) {
      mainPool = pool;
      done();
    }).fail(function(error) {
      throw error;
    }).done();
  }); 

  it('should reject with an error if create is not specified', function(done) {
    Pool({remove: function() { return; }})
      .then(function(result) {
        throw new Error('Should not return a result');
      })
      .fail(function(error) {
        error.should.be.instanceof(app.ERRORS.Pool);
        done();
      }).done();
  });

  it('should reject with an error if remove is not specified', function(done) {
    Pool({create: function() { return; }})
      .then(function(result) {
        result.should.not.exist;
      })
      .fail(function(error) {
        error.should.be.instanceof(app.ERRORS.Pool);
        done();
      }).done();
  });

  it('should return a Pool object', function(done) {
    mainPool.should.instanceof(Object);
    mainPool.acquire.should.exist;
    mainPool.release.should.exist;
    done();
  });

  it('should set the options correctly', function(done) {
    mainPool._name.should.equal('Test Pool');
    mainPool._min.should.equal(2);
    mainPool._max.should.equal(5);
    mainPool._idleTimeout.should.equal(200);
    done();
  });

  it('should create minimum clients when min is set in options', function(done) {
    mainPool.availableCount().should.equal(2);
    done();
  });

  it('should not create any clients if min is not set in options', function(done) {
    Pool({
      name  : 'Test Pool',
      create  : function(callback) { 
        return createClient();
      },
      remove  : function(client) { 
        client.end();
      },
      log   : false
    }).then(function(pool) {
        pool.availableCount().should.equal(0);
      done();
    }).fail(function(error) {
      throw error;
    }).done();
  });

  
  it('should remove any idle clients from pool', function(done) {
    var idlePool, client1, client2, client3;
    Pool({
      name  : 'Test Pool',
      create  : function(callback) { 
        return createClient();
      },
      remove  : function(client) { 
        client.end();
      },
      min     : 2,
      idleTimeout : 200,
      log     : false
    }).then(function(pool) {  
      idlePool = pool;
      return idlePool.acquire();
    }).then(function(client) {
      client1 = client;
      return idlePool.acquire();
    }).then(function(client) {
      client2 = client;
      return idlePool.acquire();
    }).then(function(client) {
      client3 = client;
      return idlePool.acquire();
    }).then(function(client) {
      idlePool.release(client1);
      idlePool.release(client2);
      idlePool.release(client3);
      idlePool.release(client);
      setTimeout(function(id) {
        idlePool.totalCount().should.equal(0);
        idlePool.availableCount().should.equal(0);
        idlePool.activeCount().should.equal(0);

        clearTimeout(id);
        done();
      }, 2000);
    }).fail(function(error) {
      throw error;
    }).done();
  });

  describe('drainNow()', function() {
    it('should drain all clients regardless of state', function(done) {
      mainPool.acquire().then(function() {
          return mainPool.acquire();
        }).then(function(client) {
          return mainPool.acquire();
        }).then(function(client) {
          return mainPool.drainNow();
        }).then(function() {
          mainPool.totalCount().should.equal(0);
          mainPool.availableCount().should.equal(0);
          mainPool.activeCount().should.equal(0);
          done();
        }).fail(function(error) {
          throw error;
        }).done();
    });
  });

  describe('acquire()', function() {
    it('should get a new client from pool when available', function(done) {
      mainPool.drainNow()
        .then(function() {
          return mainPool.acquire();
        })
        .then(function(client) {
          client.should.exist;
          mainPool.totalCount().should.equal(1);
          mainPool.availableCount().should.equal(0);
          mainPool.activeCount().should.equal(1);
          done();
        }).fail(function(error) {
          throw error;
        }).done();
    });

    it('should create a new client when none are available in the pool', function(done) {
      mainPool.drainNow()
        .then(function() {
          return mainPool.acquire();
        })
        .then(function(client) {
          client.should.exist;
          mainPool.totalCount().should.equal(1);
          mainPool.availableCount().should.equal(0);
          mainPool.activeCount().should.equal(1);
          
          return mainPool.acquire();
        })
        .then(function(client) {
          client.should.exist;
          mainPool.totalCount().should.equal(2);
          mainPool.availableCount().should.equal(0);
          mainPool.activeCount().should.equal(2);
          done();
        })
        .fail(function(error) {
          throw error;
        }).done();
    });

    it('should respect the max connections', function(done) {
      mainPool.drainNow()
        .then(function() {
          return mainPool.acquire();
        })
        .then(function(client) {          
          return mainPool.acquire();
        })
        .then(function(client) {          
          return mainPool.acquire();
        })
        .then(function(client) {          
          return mainPool.acquire();
        })
        .then(function(client) {          
          return mainPool.acquire();
        })
        .then(function(client) {
          client.should.exist;
          mainPool.totalCount().should.equal(5);
          mainPool.availableCount().should.equal(0);
          mainPool.activeCount().should.equal(5);
          
          mainPool.acquire().then(function(client) {
            mainPool.totalCount().should.equal(5);
            mainPool.availableCount().should.equal(0);
            mainPool.activeCount().should.equal(5);
            mainPool.waitingCount().should.equal(0);
            done();
          }).fail(function(error) {
            throw error;
          }).done();

          mainPool.totalCount().should.equal(5);
          mainPool.availableCount().should.equal(0);
          mainPool.activeCount().should.equal(5);
          mainPool.waitingCount().should.equal(1);

          mainPool.release(client);
        })
        .fail(function(error) {
          throw error;
        }).done();
    });
  });

  describe('release()', function() {
    it('should release the client back to the pool', function(done) {
      mainPool.drainNow()
        .then(function() {
          return mainPool.acquire();
        })
        .then(function(client) {
          client.should.exist;
          mainPool.totalCount().should.equal(1);
          mainPool.availableCount().should.equal(0);
          mainPool.activeCount().should.equal(1);
          
          mainPool.release(client);
          mainPool.totalCount().should.equal(1);
          mainPool.availableCount().should.equal(1);
          mainPool.activeCount().should.equal(0);
          done();
        }).fail(function(error) {
          throw error;
        }).done();
    });
  });

  after(function() {
    
  });
});