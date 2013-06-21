var PoolError = app.ERRORS.Pool;
var async = require('async');

function Pool(options, callback) {
  if (! options.create ) 
    return callback(new PoolError('Must specify create functions in options'));

  if (! options.remove ) 
    return callback(new PoolError('Must specify remove functions in options'));

  // Ensure 'this' is always correct
  var instance = {

    // Queues
    _available : [],
    _active    : [],
    _waiting   : [],
    
    // Set the options
    _name         : options.name,
    _idleTimeout  : options.idleTimeout || 3000,
    _max          : options.max || null,
    _min          : options.min || 0,
    _log          : options.log || false,
    _create       : options.create,
    _remove       : options.remove,
    _reapInterval : options.reapInterval || 1000,
    
    // General
    _id            : Pool.id++,
    _clientId      : 1,
    _draining      : false,
    _removingIdle  : false,
    _idleIntHandle : null,

    /**
     * Acquires a client from the pool, if one is not availabe it creates a new one
     * @return {promise} resolves with client, new or reused from pool
     */
    acquire : function(isInternal, callback) {
      var self     = this;
      var internal = (isInternal === 'undefined') ? isInternal : false;

      // If draining, stop acquire
      if (self._draining)
        return callback(new PoolError('Acquiring Client Error: Pool is draining, no clients can be acquired'));

      if (self._available.length > 0) {
        var client = self._available.shift();

        if (self._checkTimeout(client)) {
          self._debug('Reusing Client (ID: ' + client.id + ')');
          return self._dispense(client.connection, isInternal, callback);
        } 

        self._debug('Client ' + client.id + ' has timed out, removing from pool');
        self._removeFromPool(client);
        return self.acquire(isInternal, callback);
      } 

      // Push into the waiting queue
      if ( self._max && (self.totalCount() >= self._max) ) {
        return self._addToWaiting(callback);
      } 

      // If none are available, create a new client
      return self._createClient(function(client) {
        self._debug('Using new client (ID: ' + client.id + ')');
        self._dispense(client, isInternal, callback);
      });
    },

    /**
     * Returns the number of active clients in the pool
     * @return {integer} active number 
     */
    activeCount : function() {
      return this._active.length;
    },

    /**
     * Add a connection to the active array
     * @param {object} connection the database connection object
     */
    _addToActive : function(connection) {
      this._active.push(connection);
    },

    /**
     * Adds a connection to the pool
     * @param {object} connection a database connection object
     */
    _addToPool : function(connection) {
      removeIdles = (typeof removeIdles === 'undefined') ? true : removeIdles;
      var client  = {
        connection : connection,
        timeout    : new Date().getTime() + this._idleTimeout,
        id         : this._clientId++
      };

      this._available.push(client);
      this._startRemoveIdleInterval();

      this._debug('Client (' + client.id + ') to Pool');      
      return client;
    },

    /**
     * Add a promise to the wait array and send promise
     */
    _addToWaiting : function(callback) {
      this._waiting.push(callback);
      this._debug('Max clients, waiting');
    },

    /**
     * Returns the number of available clients in the pool
     * @return {integer} availabe number 
     */
    availableCount : function() {
      return this._available.length;
    },

    /**
     * Checks if the client's timeout has past
     * @param  {object} client the client object from the pool
     * @return {boolean}        TRUE if timeout is past, FALSE otherwise
     */
    _checkTimeout : function(client) {
      var currentTime = new Date().getTime();
      var timeout     = client.timeout;

      // console.log('currentTime:', currentTime, '- Client timeout:', timeout);
      return timeout >= currentTime;
    },

    /**
     * Creates a client and puts it into the active array
     * @return {promise}     resolves with new client
     */
    _createClient : function(callback) {
      this._create(callback);
    },

    /**
     * Prints debugging info if 'log' is set in options
     */
    _debug : function(msg) {
      if ( this._log )
        app.debug('*** Pool (' + this._id + ') ****:', msg, "\t\t- Total Count:", this.totalCount(), "\t- Available Count:", this.availableCount(), "\t- Active Count:", this.activeCount(), "\t- Waiting Count:", this.waitingCount());
    },

    /**
     * Decides who to dispense the client to
     * @param  {object} client the client connection object
     * @return {promise}       either a promise or immediately resolved promise
     */
    _dispense : function(client, internal, callback) {
      this._addToActive(client);

      if (this._waiting.length < 1) {
        this._debug("Send to client");
        return callback(null, client);
      }

      this._waiting.shift(function() {
        this._debug("Send waiting client");
        callback(null, client);
      });

      if (!internal)
        return this._addToWaiting();
    },

    /**
     * Drains the pool, forcibly
     * @return {promise}   
     */
    drainNow : function(callback) {
      var self = this;

      self._debug('Draining the pool');
      self._draining = true;

      // Stop the removal of idle clients, it won't matter once everything is drained
      self._stopRemoveIdleInterval();

      // Drain everything
      app.asyncForEach(self._available, function(index, client, poolList) {
        self._debug('Removing pooled client ' + client.id);
        self._remove(client.connection);
      }, function() {
        self._available = [];

        app.asyncForEach(self._active, function(index, connection, progressList) {
          self._debug('Removing active client');
          self._remove(connection);
        }, function() {
          self._active   = [];
          self._draining = false;

          self._debug('Pool drained');
          callback(null, true);
        });
      });
    },

    /**
     * Initialize the Pool
     * @return {promise} resolves with the Pool object
     */
    init : function(callback) {
      var self  = this;
      var count = new Array(self._min);

      self._debug('Initiating Pool ' + self._id);
      var tasks = count.map(function(item) {
        return function() {
          self._create(function(error, connection) {
            if (error) throw error;

            self._addToPool(connection);
          });
        }
      });

      async.parallel(tasks, function(error) {
        callback(null, self);
      });

      /*app.asyncForEach(new Array(self._min), function() {
        promises.push(
          Q.resolve(self._create())
            .then(function(connection) {
              return self._addToPool(connection);
            })
            .then(function(client) {
              self._debug('Creating client ' + client.id);
            })
        );
      }, function() {
        Q.allResolved(promises).then(function(promises) {
          deferred.resolve(self);
        });
      });

      return deferred.promise;*/
    },

    /**
     * Releases a connection back into the pool for later use
     * @param  {object} connection a database connection object
     */
    release : function(connection) {
      if (this._active.indexOf(connection) < 0)
        return;

      this._debug('Releasing Client');
      this._removeFromActive(connection); 
      var client = this._addToPool(connection);

      if (this._waiting.length > 0)
        this.acquire(true);
    },

    /**
     * Remove the client from the pool for good
     * @param  {object} client the client object to remove
     * @return {promise}        
     */
    _removeFromPool : function(client) {
      this._remove(client.connection);
      this._available.splice(this._available.indexOf(client), 1);
      this._debug('Client ' + client.id + ' Removed (timeout)');
    },

    /**
     * Remove the connection from the active array
     * @param  {object} connection the connection object to remove
     * @return {promise}        
     */
    _removeFromActive : function(connection) {
      this._active.splice(this._active.indexOf(connection), 1);
    },

    /**
     * Start the removal of idle clients from the pool
     */
    _startRemoveIdleInterval : function() {
      if (this._removingIdle || (this._available.length < 1) )
        return;

      this._removingIdle = true;
      this._debug('Start check idle');
      this._idleIntHandle = setTimeout(function() {
        if (this._available.length <= this._min) {
          this._startRemoveIdleInterval();
          return;
        }

        var removeClients = [];
        app.asyncForEach(this._available, function(index, client, clientList) {
          if (this._available.length > this._min) {
            this._debug('Checking client ' + client.id + ' for timeout');
            if ( (!this._checkTimeout(client)) )
              removeClients.push(client);
          }
        }.bind(this), function() {
          app.asyncForEach(removeClients, function(index, removeClient, removeClientList) {
            this._removeFromPool(removeClient);
          }.bind(this), function() {
            this._removingIdle = false;
            this._startRemoveIdleInterval();
          }.bind(this));
        }.bind(this));
      }.bind(this), this._reapInterval);
    },

    /**
     * Stop the removal of pool clients
     */
    _stopRemoveIdleInterval : function() {
      if (! this._removingIdle )
        return;

      clearTimeout(this._idleIntHandle);
      this._removingIdle  = false;
      this._idleIntHandle = null;
    },

    /**
     * Tests whether a connection can be made
     * @return {promise} 
     */
    testConnection : function(callback) {
      return this._createClient(function(client) {
        client.testConnection(callback);
      });
    },

    /**
     * Returns the number of total clients in the pool and in progress
     * @return {integer} total number 
     */
    totalCount : function() {
      return this._available.length + this._active.length;
    },

    /**
     * Returns the number of waiting acquires
     * @return {integer} availabe number 
     */
    waitingCount : function() {
      return this._waiting.length;
    },
  };

  instance.init(callback);
}

Pool.id = 1;


module.exports = Pool;