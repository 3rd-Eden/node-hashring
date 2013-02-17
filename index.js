'use strict';

var hashValue = require('./build/Release/hashvalue')
  , parse = require('connection-parse')
  , crypto = require('crypto');

function HashRing(servers) {
  this.algorithm = 'md5';

  // Private propperties
  this.servers = parse(servers).servers;
  this.ring = [];
  this.size = 0;

  this.generate();
}

HashRing.prototype.digest = function digest(key) {
  return crypto.createHash(this.algorithm)
    .update(key +'', 'utf8').digest()
    .split('')
    .map(function charCode(char) {
      return char.charCodeAt(0);
    });
};

HashRing.prototype.hashValue = function hasher(key) {
  var x = this.digest(key);

  return hashValue.hash(x[3], x[2], x[1], x[0]);
};

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

function Node(point, server) {
  this.point = point;
  this.server = server;
}

module.exports = HashRing;
