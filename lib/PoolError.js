module.exports = PoolError;

/**
 * Pool error
 * @param {string} msg    the error message
 * @param {[type]} constr 
 */
function PoolError(msg, log) {}

// Inherit from Error
require('util').inherits(PoolError, Error);

PoolError.prototype.name = 'PoolError';