'use strict';

var hashValue = require('./build/Release/hashvalue')
  , SimpleCache = require("simple-lru-cache")
  , parse = require('connection-parse')
  , crypto = require('crypto');

/**
 * Add a virtual node parser to the connection string parser.
 *
 * @param {Object} data server data
 * @param {Mixed} value optional value
 * @api private
 */
parse.extension('vnodes', function vnode(data, value) {
  if (value && 'vnodes' in value) {
    data.vnodes = +value.vnodes || 0;
  } else {
    data.vnodes = 0;
  }
});

/**
 * HashRing implements consistent hashing so adding or removing servers of one
 * slot does not significantly change the mapping of the key to slots. The
 * consistent hashing algorithm is based on ketama or libketama.
 *
 * @constructor
 * @param {Mixed} server Servers that need to be added to the ring
 * @param {Mixed} algorithm Either a Crypto compatible algorithm or custom hasher
 * @param {Object} options Optional configuration and options for the ring
 */
function HashRing(servers, algorithm, options) {
  options = options || {};

  // These properties can be configured
  this.algorithm = algorithm || 'md5';
  this.vnode = options.vnode_count || 40;

  // Private properties
  var connections = parse(servers);

  this.ring = [];
  this.size = 0;
  this.vnodes = connections.vnodes;
  this.servers = connections.servers;

  // Set up a ache as we don't want to preform a hashing operation every single
  // time we lookup a key.
  this.cache = new SimpleCache({
    maxSize: options.maxCacheSize || 5000
  });

  // Override the hashing function if people want to use a hashing algorithm
  // that is not supported by Node, for example if you want to MurMur hashing or
  // something else exotic.
  if ('function' === typeof this.algorithm) {
    this.hash = this.algorithm;
  }

  // Generate the continuum of the HashRing.
  this.continuum();
}

/**
 * Generates the continuum of server a.k.a the Hash Ring based on their weights
 * and virtual nodes assigned.
 *
 * @api private
 */
HashRing.prototype.continuum = function generate() {
  var servers = this.servers
    , self = this
    , index = 0
    , total;

  // Generate the total weight of all the servers
  total = servers.reduce(function reduce(total, server) {
    return total += server.weight;
  }, 0);

  servers.forEach(function each(server) {
    var percentage = server.weight / total
      , vnodes = self.vnodes[server] || self.vnodes
      , length = Math.floor(percentage * vnodes * servers.length)
      , key
      , x;

    for (var i = 0; i < length; i++) {
      x = self.digest(server.string +'-'+ i);

      for (var j = 0; j < 3; j++) {
        key = hashValue.hash(x[3 + j * 4], x[2 + j * 4], x[1 + j * 4], x[j * 4]);
        self.ring[index] = new Node(key, server.string);
        index++;
      }
    }
  });

  // Sort the keys
  this.ring = this.ring.sort(function sorted(a, b) {
    return (a.value < b.value) ? -1 : (a.value > b.value) ? 1 : 0;
  });

  this.size = this.ring.length;
};

/**
 * Find the correct node for the key which is closest to the point after what
 * the given key hashes to.
 *
 * @param {String} key
 * @returns {String} server
 * @api public
 */
HashRing.prototype.get = function get(key) {
  var hashValue = this.hashValue(key)
    , ring = this.ring
    , high = this.size
    , low = 0
    , middle
    , prev
    , mid;

  // Preform a search on the array to find the server with the next biggest
  // point after what the given key hashes to
  while (true) {
    mid = (low + high) / 2;

    if (mid === this.size) return ring[0].server;

    middle = ring[mid].value;
    prev = mid === 0 ? 0 : ring[mid - 1].value;

    if (hashValue <= middle && hashValue > prev) return ring[mid].server;

    if (middle < hashValue) {
      low = mid + 1;
    } else {
      high = mid - 1;
    }

    if (low > high) return ring[0].server;
  }
};

/**
 * Generates a hash of the string.
 *
 * @param {String} key
 * @returns {String} hash
 * @api private
 */
HashRing.prototype.hash = function hash(key) {
  return crypto.createHash(this.algorithm).update(key).digest();
};

/**
 * Digest hash so we can make a numeric representation from the hash.
 *
 * @param {String} key The key that needs to be hashed
 * @returns {Array}
 * @api private
 */
HashRing.prototype.digest = function digest(key) {
  return this.hash(key +'').toString().split('').map(function charCode(char) {
    return char.charCodeAt(0);
  });
};

/**
 * None ketama:
 * The following changes are not ported from the ketama algorithm and are hash
 * ring specific. Add, remove or replace servers with as less disruption as
 * possible.
 */

HashRing.prototype.replace = function replace(from, to) {

};

HashRing.prototype.add = function add(servers) {

};

HashRing.prototype.remove = function remove(server) {

};

/**
 * Get the hashed value for the given key
 *
 * @param {String} key
 * @returns {Number}
 * @api private
 */
HashRing.prototype.hashValue = function hasher(key) {
  var x = this.digest(key);

  return hashValue.hash(x[3], x[2], x[1], x[0]);
};

/**
 * A single Node in our hash ring.
 *
 * @constructor
 * @param {Number} hashvalue
 * @param {String} server
 * @api private
 */
function Node(hashvalue, server) {
  this.value = hashvalue;
  this.server = server;
}

/**
 * Set up the legacy API aliases.
 *
 * @api public
 */
HashRing.prototype.replaceServer = HashRing.prototype.replace;
HashRing.prototype.removeServer = HashRing.prototype.remove;
HashRing.prototype.addServer = HashRing.prototype.add;
HashRing.prototype.getNode = HashRing.prototype.get;

/**
 * Expose the module.
 *
 * @api public
 */
module.exports = HashRing;
