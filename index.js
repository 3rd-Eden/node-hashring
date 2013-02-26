'use strict';

var hashValue = require('./build/Release/hashvalue')
  , SimpleCache = require("simple-lru-cache")
  , StringDecoder = require('string_decoder').StringDecoder
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
  if (typeof value === 'object' && !Array.isArray(value) && 'vnodes' in value) {
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
  this.vnode = options['vnode count'] || 40;          // Virtual nodes per server
  this.algorithm = algorithm || 'md5';                // Hashing algorithm

  // There's a slight difference between libketama and python's hash_ring
  // module, libketama creates 160 points per server:
  //
  //   40 hashes (vnodes) and 4 replicas per hash = 160 points per server
  //
  // The hash_ring module only uses 120 points per server:
  //
  //   40 hashes (vnodes) and 3 replicas per hash = 160 points per server
  //
  // And that's the only difference between the original ketama hash and the
  // hash_ring package. Small, but important.
  this.replicas = options.compatibility
    ? (options.compatibility === 'hash_ring' ? 3 : 4)
    : +options.replicas || 4;

  // Private properties
  var connections = parse(servers);

  this.ring = [];
  this.size = 0;
  this.vnodes = connections.vnodes;
  this.servers = connections.servers;

  // Set up a ache as we don't want to preform a hashing operation every single
  // time we lookup a key.
  this.cache = new SimpleCache({
    maxSize: options['max cache size'] || 5000
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
    , total = 0;

  // No servers, bailout
  if (!servers.length) return this;

  // Generate the total weight of all the servers
  total = servers.reduce(function reduce(total, server) {
    return total + server.weight;
  }, 0);

  servers.forEach(function each(server) {
    var percentage = server.weight / total
      , vnodes = self.vnodes[server.string] || self.vnode
      , length = Math.floor(percentage * vnodes * servers.length)
      , key
      , x;

    // If you supply us with a custom vnode size, we will use that instead of
    // our computed distribution
    if (vnodes !== self.vnode) length = vnodes;

    for (var i = 0; i < length; i++) {
      x = self.digest(server.string +'-'+ i);

      for (var j = 0; j < self.replicas; j++) {
        key = hashValue.hash(x[3 + j * 4], x[2 + j * 4], x[1 + j * 4], x[j * 4]);
        self.ring[index] = new Node(key, server.string);
        index++;
      }
    }
  });

  // Sort the keys using the continuum points compare that is used in ketama
  // hashing.
  this.ring = this.ring.sort(function sorted(a, b) {
    if (a.value === b.value) return 0;
    else if (a.value > b.value) return 1;

    return -1;
  });

  this.size = this.ring.length;
  return this;
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
  var cache = this.cache.get(key);
  if (cache) return cache;

  var node = this.ring[this.find(this.hashValue(key))];
  if (!node) return undefined;

  this.cache.set(key, node.server);
  return node.server;
};

/**
 * Returns the position of the hashValue in the hashring
 *
 * @param {Number} hashValue find the nearest server close to this hash
 * @returns {Number} position of the server in the hash ring
 * @api public
 */
HashRing.prototype.find = function find(hashValue) {
  var ring = this.ring
    , high = this.size
    , low = 0
    , middle
    , prev
    , mid;

  // Preform a search on the array to find the server with the next biggest
  // point after what the given key hashes to
  while (true) {
    mid = (low + high) >> 1;

    if (mid === this.size) return 0;

    middle = ring[mid].value;
    prev = mid === 0 ? 0 : ring[mid - 1].value;

    if (hashValue <= middle && hashValue > prev) return mid;

    if (middle < hashValue) {
      low = mid + 1;
    } else {
      high = mid - 1;
    }

    if (low > high) return 0;
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
  if (this.algorithm !== 'crc32')
    return crypto.createHash(this.algorithm).update(key).digest();

  // Older versions of node-memcached provide an algorithm of 'crc32' by 
  // default, and don't specify a version of hashring, so the following 
  // is necessary.
  var str = new StringDecoder('utf8').write(key);

  var crc = 0 ^ (-1)
    , i = 0
    , length = str.length
    , map = '00000000 77073096 EE0E612C 990951BA 076DC419 706AF48F E963A535 9E6495A3 0EDB8832 79DCB8A4 E0D5E91E 97D2D988 09B64C2B 7EB17CBD E7B82D07 90BF1D91 1DB71064 6AB020F2 F3B97148 84BE41DE 1ADAD47D 6DDDE4EB F4D4B551 83D385C7 136C9856 646BA8C0 FD62F97A 8A65C9EC 14015C4F 63066CD9 FA0F3D63 8D080DF5 3B6E20C8 4C69105E D56041E4 A2677172 3C03E4D1 4B04D447 D20D85FD A50AB56B 35B5A8FA 42B2986C DBBBC9D6 ACBCF940 32D86CE3 45DF5C75 DCD60DCF ABD13D59 26D930AC 51DE003A C8D75180 BFD06116 21B4F4B5 56B3C423 CFBA9599 B8BDA50F 2802B89E 5F058808 C60CD9B2 B10BE924 2F6F7C87 58684C11 C1611DAB B6662D3D 76DC4190 01DB7106 98D220BC EFD5102A 71B18589 06B6B51F 9FBFE4A5 E8B8D433 7807C9A2 0F00F934 9609A88E E10E9818 7F6A0DBB 086D3D2D 91646C97 E6635C01 6B6B51F4 1C6C6162 856530D8 F262004E 6C0695ED 1B01A57B 8208F4C1 F50FC457 65B0D9C6 12B7E950 8BBEB8EA FCB9887C 62DD1DDF 15DA2D49 8CD37CF3 FBD44C65 4DB26158 3AB551CE A3BC0074 D4BB30E2 4ADFA541 3DD895D7 A4D1C46D D3D6F4FB 4369E96A 346ED9FC AD678846 DA60B8D0 44042D73 33031DE5 AA0A4C5F DD0D7CC9 5005713C 270241AA BE0B1010 C90C2086 5768B525 206F85B3 B966D409 CE61E49F 5EDEF90E 29D9C998 B0D09822 C7D7A8B4 59B33D17 2EB40D81 B7BD5C3B C0BA6CAD EDB88320 9ABFB3B6 03B6E20C 74B1D29A EAD54739 9DD277AF 04DB2615 73DC1683 E3630B12 94643B84 0D6D6A3E 7A6A5AA8 E40ECF0B 9309FF9D 0A00AE27 7D079EB1 F00F9344 8708A3D2 1E01F268 6906C2FE F762575D 806567CB 196C3671 6E6B06E7 FED41B76 89D32BE0 10DA7A5A 67DD4ACC F9B9DF6F 8EBEEFF9 17B7BE43 60B08ED5 D6D6A3E8 A1D1937E 38D8C2C4 4FDFF252 D1BB67F1 A6BC5767 3FB506DD 48B2364B D80D2BDA AF0A1B4C 36034AF6 41047A60 DF60EFC3 A867DF55 316E8EEF 4669BE79 CB61B38C BC66831A 256FD2A0 5268E236 CC0C7795 BB0B4703 220216B9 5505262F C5BA3BBE B2BD0B28 2BB45A92 5CB36A04 C2D7FFA7 B5D0CF31 2CD99E8B 5BDEAE1D 9B64C2B0 EC63F226 756AA39C 026D930A 9C0906A9 EB0E363F 72076785 05005713 95BF4A82 E2B87A14 7BB12BAE 0CB61B38 92D28E9B E5D5BE0D 7CDCEFB7 0BDBDF21 86D3D2D4 F1D4E242 68DDB3F8 1FDA836E 81BE16CD F6B9265B 6FB077E1 18B74777 88085AE6 FF0F6A70 66063BCA 11010B5C 8F659EFF F862AE69 616BFFD3 166CCF45 A00AE278 D70DD2EE 4E048354 3903B3C2 A7672661 D06016F7 4969474D 3E6E77DB AED16A4A D9D65ADC 40DF0B66 37D83BF0 A9BCAE53 DEBB9EC5 47B2CF7F 30B5FFE9 BDBDF21C CABAC28A 53B39330 24B4A3A6 BAD03605 CDD70693 54DE5729 23D967BF B3667A2E C4614AB8 5D681B02 2A6F2B94 B40BBE37 C30C8EA1 5A05DF1B 2D02EF8D';

  for (; i < length; i++) {
    crc = (crc >>> 8) ^ ('0x' + map.substr(((crc ^ str.charCodeAt(i)) & 0xFF) * 9, 8));
  }

  crc = crc ^ (-1);

  return (crc < 0 ? crc += 4294967296 : crc).toString()
    .split('')
    .map(function map (v) {
      return v.charCodeAt(0);
    });
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
 * None ketama:
 *
 * The following changes are not ported from the ketama algorithm and are hash
 * ring specific. Add, remove or replace servers with as less disruption as
 * possible.
 */

/**
 * Get a range of different servers.
 *
 * @param {String} key
 * @param {Number} size Amount of servers it should return
 * @param {Boolean} unique Return only unique keys
 * @return {Array}
 * @api public
 */
HashRing.prototype.range = function range(key, size, unique) {
  if (!this.size) return [];

  size = size || this.servers.length;
  unique = unique || 'undefined' === typeof unique;

  var position = this.find(this.hashValue(key))
    , length = this.ring.length
    , servers = []
    , node;

  // Start searching for servers from the postion of the key to the end of
  // HashRing.
  for (var i = position; i < length; i++) {
    node = this.ring[i];

    // Do we need to make sure that we retrieve a unique list of servers?
    if (unique) {
      if (!~servers.indexOf(node.server)) servers.push(node.server);
    } else {
      servers.push(node.server);
    }

    if (servers.length === size) return servers;
  }

  // Not enough results yet, so iterate from the start of the hash ring to the
  // position of the hash ring. So we reach full circle again.
  for (i = 0; i < position; i++) {
    node = this.ring[i];

    // Do we need to make sure that we retrieve a unique list of servers?
    if (unique) {
      if (!~servers.indexOf(node.server)) servers.push(node.server);
    } else {
      servers.push(node.server);
    }

    if (servers.length === size) return servers;
  }

  return servers;
};

/**
 * Returns the points per server.
 *
 * @param {String} server Optional server to filter down
 * @returns {Object} server -> Array(points)
 * @api public
 */
HashRing.prototype.points = function points(servers) {
  servers = Array.isArray(servers) ? servers : Object.keys(this.vnodes);

  var nodes = Object.create(null)
    , node;

  servers.forEach(function servers(server) {
    nodes[server] = [];
  });

  for (var i = 0; i < this.size; i++) {
    node = this.ring[i];

    if (node.server in nodes) {
      nodes[node.server].push(node.value);
    }
  }

  return nodes;
};

/**
 * Hotswap identical servers with each other. This doesn't require the cache to
 * be completely nuked and the hash ring distribution to be re-calculated.
 *
 * Please note that removing the server and a new adding server could
 * potentially create a different distribution.
 *
 * @param {String} from The server that needs to be replaced
 * @param {String} to The server that replaces the server
 * @api public
 */
HashRing.prototype.swap = function swap(from, to) {
  var connection = parse(to).servers.pop()
    , self = this;

  this.ring.forEach(function forEach(node) {
    if (node.server === from) node.server = to;
  });

  this.cache.forEach(function forEach(value, key) {
    if (value === from) self.cache.set(key, to);
  }, this);

  // Update the virtual nodes
  this.vnodes[to] = this.vnodes[from];
  delete this.vnodes[from];

  // Update the servers
  this.servers = this.servers.map(function mapswap(server) {
    if (server.string === from) {
      server.string = to;
      server.host = connection.host;
      server.port = connection.port;
    }

    return server;
  });

  return this;
};

/**
 * Add a new server to ring without having to re-initialize the hashring. It
 * accepts the same arguments as you can use in the constructor.
 *
 * @param {Mixed} servers Servers that need to be added to the ring
 * @api public
 */
HashRing.prototype.add = function add(servers) {
  var connections = Object.create(null);

  // Add the current servers to the set.
  this.servers.forEach(function forEach(server) {
    connections[server.string] = server;
  });

  parse(servers).servers.forEach(function forEach(server) {
    // Don't add duplicate servers
    if (server.string in connections) return;
    connections[server.string] = server;
  });

  // Now that we generated a complete set of servers, we can update the re-parse
  // the set and correctly added all the servers again
  connections = parse(connections);
  this.vnodes = connections.vnodes;
  this.servers = connections.servers;

  // Rebuild the hash ring
  this.reset();
  return this.continuum();
};

/**
 * Remove a server from the hashring.
 *
 * @param {Mixed} server
 * @api public
 */
HashRing.prototype.remove = function remove(server) {
  var connection = parse(server).servers.pop();

  delete this.vnodes[connection.string];
  this.servers = this.servers.map(function map(server) {
    if (server.string === connection.string) return undefined;

    return server;
  }).filter(Boolean);

  // Rebuild the hash ring
  this.reset();
  return this.continuum();
};

/**
 * Reset the HashRing to clean up all references
 *
 * @api public
 */
HashRing.prototype.reset = function reset() {
  this.ring.length = 0;
  this.size = 0;
  this.cache.reset();

  return this;
};

HashRing.prototype.end = function end() {
  this.reset();

  this.vnodes = {};
  this.servers.length = 0;

  return this;
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
 * Set up the legacy API aliases. These will be depricated in the next release.
 *
 * @api public
 */
[
  { from: 'replaceServer' },
  { from: 'replace' },
  { from: 'removeServer', to: 'remove' },
  { from: 'addServer', to: 'add' },
  { from: 'getNode', to: 'get' },
  { from: 'getNodePosition', to: 'find' },
  { from: 'position', to: 'find' }
].forEach(function depricate(api) {
  var notified = false;

  HashRing.prototype[api.from] = function depricating() {
    if (!notified) {
      console.warn();
      console.warn('[depricated] HashRing#'+ api.from +' is removed.');

      // Not every API has a replacement API that should be used
      if (api.to) {
        console.warn('[depricated] use HashRing#'+ api.to +' as replacement.');
      } else {
        console.warn('[depricated] the API has no replacement');
      }

      console.warn();
      notified = true;
    }

    if (api.to) return HashRing.prototype[api.to].apply(this, arguments);
  };
});

/**
 * Expose the current version number.
 *
 * @type {String}
 */
HashRing.version = require('./package.json').version;

/**
 * Expose the module.
 *
 * @api public
 */
module.exports = HashRing;
