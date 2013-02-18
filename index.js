'use strict';

var hashValue = require('./build/Release/hashvalue')
  , SimpleCache = require("simple-lru-cache")
  , parse = require('connection-parse')
  , crypto = require('crypto');

function HashRing(servers, algorithm, options) {
  options = options || {};

  this.algorithm = algorithm || 'md5';

  // Private propperties
  this.ring = [];
  this.size = 0;
  this.servers = parse(servers).servers;
  this.cache = new SimpleCache({
    maxSize: options.maxCacheSize || 5000
  });

  // Override our hasher, which defaults to hashing using the `crypto` module by
  // default.
  if ('crc32' === this.algorithm) {
    this.hash = this.crc32;
  } else if ('function' === typeof this.algorithm) {
    this.hash = this.algorithm;
  }

  // Initialize the hash ring
  this.generate();
}

HashRing.prototype.generate = function generate() {
  var servers = this.servers
    , self = this;

  // Generate the total weight of all the servers
  var total = servers.reduce(function reduce(total, server) {
    return total += server.weight;
  }, 0);

  var index = 0;

  servers.forEach(function each(server) {
    var percentage = server.weight / total
      , length = Math.floor(percentage * 40 * servers.length)
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
    return (a.point < b.point)
      ? -1
      : (a.point > b.point)
        ? 1
        : 0;
  });

  this.size = this.ring.length;
};

HashRing.prototype.get = function get(key) {
  var hashValue = this.hashValue(key)
    , ring = this.ring
    , high = this.size
    , low = 0
    , mid;

  // Preform bisection on the array
  while (true) {
    mid = (low + high) >> 1;

    if (mid === this.size) return ring[0].server;

    var middle = ring[mid].point;
    var prev = mid === 0 ? 0 : ring[mid - 1].point;

    if (hashValue <= middle && hashValue > prev) return ring[mid].server;

    if (middle < hashValue) {
      low = mid + 1;
    } else {
      high = mid - 1;
    }

    if (low > high) return ring[0].server;
  }
};

HashRing.prototype.hash = function hash(key) {
  return crypto.createHash(this.algorithm).update().digest();
};

HashRing.prototype.digest = function digest(key) {
  return this.hash(key +'').toString().split('').map(function charCode(char) {
    return char.charCodeAt(0);
  });
};

HashRing.prototype.hashValue = function hasher(key) {
  var x = this.digest(key);

  return hashValue.hash(x[3], x[2], x[1], x[0]);
};

function Node(point, server) {
  this.point = point;
  this.server = server;
}

module.exports = HashRing;
