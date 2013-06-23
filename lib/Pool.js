var PoolError = require('./PoolError')
var async     = require('async');

var MAX_POOL_ID   = 1000000;
var MAX_CLIENT_ID = 1000000;

module.exports = Pool;

function Pool(options, callback) {
  if (! options.create ) 
    return callback(new PoolError("Must specify 'create' function in options"));

  if (! options.destroy ) 
    return callback(new PoolError("Must specify 'destroy' function in options"));

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
    _destroy      : options.destroy,
    _reapInterval : options.reapInterval || 1000,
    
    // General
    _id            : (Pool.id < MAX_POOL_ID) ? Pool.id++ : 1,
    _clientId      : 1,
    _draining      : false,
    _removingIdle  : false,
    _idleIntHandle : null,

    /**
     * Acquires a client from the pool, if one is not availabe it creates a new one
     * @return {client} 
     */
    acquire : function(callback) {
      var self = this;

      // If draining, stop acquire
      if (self._draining)
        return callback(new PoolError('Acquiring Client Error: Pool is draining, no clients can be acquired'));

      self._debug('Acquiring client');

      // If there are any available clients, use them
      if (self._available.length > 0) {
        var client = self._available.shift();

        return self._checkTimeout(client, function(timedOut) {
          if (timedOut) {
            self._debug('Client ' + client.id + ' has timed out, removing from pool');
            self._removeFromPool(client);
            return self.acquire(isInternal, callback);
          }

          self._debug('Reusing Client (' + client.id + ')');
          self._dispense(client.connection, callback);
        });
      } 

      // Hit the max, push into the waiting queue
      if ( self._max && (self.totalCount() >= self._max) ) {
        return self._addToWaiting(callback);
      } 

      // If none are available, create a new client
      self._createClient(function(error, client) {
        self._debug('Using new client (' + client.id + ')');
        self._dispense(client, callback);
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
    _addToPool : function(connection, callback) {
      removeIdles = (typeof removeIdles === 'undefined') ? true : removeIdles;
      var client  = {
        connection : connection,
        timeout    : new Date().getTime() + this._idleTimeout,
        id         : this._clientId++
      };

      this._debug('Client (' + client.id + ') to Pool');  
      this._available.push(client);
      this._startRemoveIdleInterval();
    
      callback(null, client);
    },

    /**
     * Add a callback to the waiting array
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
     * @return {boolean}        TRUE if timeout is past and we are above the min value, FALSE otherwise
     */
    _checkTimeout : function(client, callback) {
      var timeLeft = client.timeout - new Date().getTime();
      this._debug('\tChecking client ' + client.id + ' for timeout (' + timeLeft + 'ms left)');
      callback( (timeLeft < 1) && (this._available.length > this._min));
    },

    /**
     * Creates a client and puts it into the active array
     * @return {client}   
     */
    _createClient : function(callback) {
      this._create(function(error, client) {
        if (!client.hasOwnProperty('id'))
          client.id = (Pool.client_id < MAX_CLIENT_ID) ? Pool.client_id++ : 1;
        callback(error, client);
      });
    },

    /**
     * Prints debugging info if 'log' is set in options
     */
    _debug : function(msg) {
      if ( this._log )
        console.log('*** Pool (' + this._id + ') ****:', msg, "- Total Count:", this.totalCount(), "- Available Count:", this.availableCount(), "- Active Count:", this.activeCount(), "- Waiting Count:", this.waitingCount());
    },

    /**
     * Decides who to dispense the client to
     * @param  {object} client the client connection object
     * @return {client}      
     */
    _dispense : function(client, callback) {
      this._addToActive(client);
      return callback(null, client);
    },

    /**
     * Drains the pool, forcibly
     */
    drain : function(callback) {
      var self = this;

      self._debug('Draining the pool');
      self._draining = true;

      // Stop the removal of idle clients, it won't matter once everything is drained
      self._stopRemoveIdleInterval();

      // Drain everything
      async.each(self._available, function(client, callback) {
        self._debug('Removing pooled client ' + client.id);
        self._destroy(client.connection, callback);
      }, function() {
        self._available = [];

        async.each(self._active, function(connection, callback) {
          self._debug('Removing active client');
          self._destroy(connection, callback);
        }, function() {
          self._active   = [];
          self._draining = false;

          self._debug('Pool drained');
          callback();
        });
      });
    },

    /**
     * Initialize the Pool
     * @return {object} with the Pool instance
     */
    init : function(callback) {
      var self  = this;
      var count = new Array(self._min+1).join('0').split('').map(parseFloat)

      self._debug('Initiating Pool with ' + self._min + ' clients');
      var tasks = count.map(function(item) {
        self._debug('Creating task:', item);
        return function(cb) {
          self._create(function(error, connection) {
            if (error) throw error;

            self._addToPool(connection, cb);
          });
        }
      });

      async.parallel(tasks, function(error) {
        callback(null, self);
      });
    },

    /**
     * Releases a connection back into the pool for later use
     * @param  {client} 
     */
    release : function(connection, callback) {
      if (this._active.indexOf(connection) < 0)
        return;

      var self = this;
      this._debug('Releasing Client');
      this._removeFromActive(connection, function() {
        self._addToPool(connection, function(error, client) {
          if (self._waiting.length > 0)
            self.acquire(self._waiting.shift());  

          if (callback)
            callback(null, client);
        });
      }); 
    },

    /**
     * Remove the client from the pool for good
     * @param  {object} client the client object to remove
     */
    _removeFromPool : function(client, callback) {
      var self = this;
      this._destroy(client.connection, function() {
        self._available.splice(self._available.indexOf(client), 1);
        self._debug('Client ' + client.id + ' Removed (timeout)');
        
        if (callback)
          callback();
      });
    },

    /**
     * Remove the connection from the active array
     * @param  {object} connection the connection object to remove
     */
    _removeFromActive : function(connection, callback) {
      this._active.splice(this._active.indexOf(connection), 1);
      
      if (callback)
        callback();
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
        if (this._available.length <= this._min) 
          return this._startRemoveIdleInterval();

        var removeClients = [];
        async.each(this._available, function(client, callback) {
          this._checkTimeout(client, function(timedOut) {
            if (timedOut)
              removeClients.push(client);

            callback(null);
          })
        }.bind(this), function() {
          async.each(removeClients, function(client, callback) {
            this._removeFromPool(client, callback);
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
     * @return {Function} 
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

Pool.id        = 1;
Pool.client_id = 1;