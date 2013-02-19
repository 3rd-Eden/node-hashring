"use strict";

var Hashring = require('../');

module.exports = {
  'Library version': function () {
    Hashring.version.should.match(/^\d+\.\d+\.\d+$/);
  }

, 'Constructing with a string': function () {
    var ring = new Hashring('192.168.0.102:11212');

    ring.servers.should.have.length(1);
    ring.ring.length.should.be.above(1);
  }

, 'Constructing with a array': function () {
    var ring = new Hashring([
        '192.168.0.102:11212'
      , '192.168.0.103:11212'
      , '192.168.0.104:11212'
    ]);

    ring.servers.should.have.length(3);
    ring.ring.length.should.be.above(1);
  }

, 'Constructing with a object': function () {
    var ring = new Hashring({
        '192.168.0.102:11212': 2
      , '192.168.0.103:11212': 2
      , '192.168.0.104:11212': 2
    });

    ring.servers.should.have.length(3);
    ring.ring.length.should.be.above(1);

    ring.servers.forEach(function (server) {
      server.weight.should.be.above(1);
    });
  }

, 'Constructing with a object with pernode vnodes': function(){
    var ring = new Hashring({
        '192.168.0.102:11212': { 'vnodes': 40 }
      , '192.168.0.103:11212': { 'vnodes': 50 }
      , '192.168.0.104:11212': { 'vnodes': 5 }
    });

    ring.servers.should.have.length(3);
    ring.ring.length.should.be.equal((40 + 50 + 5) * 3);

    ring = new Hashring({
        '192.168.0.102:11212': { 'vnodes': 4 }
      , '192.168.0.103:11212': { 'vnodes': 3 }
      , '192.168.0.104:11212': { 'vnodes': 5 }
    });

    ring.servers.should.have.length(3);
    ring.ring.length.should.be.equal((4 + 3 + 5) * 3);
  }

, 'Constructing with a different algorithm': function () {
    var ring = new Hashring('192.168.0.102:11212', 'md5');

    ring.servers.should.have.length(1);
    ring.algorithm.should.equal('md5');
    ring.ring.length.should.be.above(1);
  }

, 'Constructing with no arguments': function () {
    var ring = new Hashring();

    ring.servers.should.have.length(0);
    ring.ring.should.have.length(0);
  }

, 'Adding server after zero-argument constructor': function () {
    var ring = new Hashring();
    ring.add('192.168.0.102:11212');

    ring.servers.should.have.length(1);
    ring.ring.length.should.be.above(1);
  }

, 'Looking up keys': function () {
    var ring = new Hashring([
        '192.168.0.102:11212'
      , '192.168.0.103:11212'
      , '192.168.0.104:11212'
    ]);

    ring.find(ring.hashValue('foo')).should.be.above(-1);

    // NOTE we are going to do some flaky testing ;P
    ring.get('foo').should.equal('192.168.0.102:11212');
    ring.get('pewpew').should.equal('192.168.0.104:11212');

    // we are not gonna verify the results we are just gonna test if we don't
    // fuck something up in the code, so it throws errors or whatever

    // unicode keys, just because people roll like that
    ring.find(ring.hashValue('привет мир, Memcached и nodejs для победы')).should.be.above(-1);

    // other odd keys
    ring.find(ring.hashValue(1)).should.be.above(-1);
    ring.find(ring.hashValue(0)).should.be.above(-1);
    ring.find(ring.hashValue([])).should.be.above(-1);
    ring.find(ring.hashValue({wtf:'lol'})).should.be.above(-1);

    ring.get({wtf:'lol'}).should.equal(ring.get({wtf:'amazing .toStringing'}));
  }

, 'Swapping servers': function () {
    var ring = new Hashring([
          '192.168.0.102:11212'
        , '192.168.0.103:11212'
        , '192.168.0.104:11212'
      ])
      , amazon = ring.get('justdied')
      , skynet = '192.168.0.128:11212';

    ring.swap(amazon, skynet);
    ring.cache.get("justdied").should.equal(skynet);

    // After a cleared cache, it should still resolve to the same server
    ring.cache.reset();
    ring.get('justdied').should.equal(skynet);
  }

, 'Removing servers': function () {
    var ring = new Hashring([
        '192.168.0.102:11212'
      , '192.168.0.103:11212'
      , '192.168.0.104:11212'
    ]);

    ring.remove('192.168.0.102:11212');
    ring.ring.forEach(function (node) {
      node.server.should.not.equal('192.168.0.102:11212');
    });
  }

, 'Removing last server': function () {
    var ring = new Hashring('192.168.0.102:11212');
    ring.remove('192.168.0.102:11212');

    ring.servers.should.have.length(0);
    ring.ring.should.have.length(0);
  }

  // kindly lended from `node-hash-ring` :)
, 'Distribution': function () {
    var iterations = 100000
      , nodes = {
            '192.168.0.102:11212': 1
          , '192.168.0.103:11212': 1
          , '192.168.0.104:11212': 1
        }
      , ring = new Hashring(nodes);

    function genCode (length) {
      length = length || 10;
      var chars = "QWERTYUIOPASDFGHJKLZXCVBNMqwertyuiopasdfghjklzxcvbnm1234567890"
        , numChars = chars.length
        , ret = ""
        , i = 0;

      for (; i < length; i++) {
          ret += chars[parseInt(Math.random() * numChars, 10)];
      }

      return ret;
    }

    var counts = {}
      , node
      , i
      , len
      , word;

    for (i = 0, len = nodes.length; i < len; i++) {
        node = nodes[i];
        counts[node] = 0;
    }

    for (i = 0, len = iterations; i < len; i++) {
      word = genCode(10);
      node = ring.get(word);
      counts[node] = counts[node] || 0;
      counts[node]++;
    }

    var total = Object.keys(counts).reduce(function reduce (sum, node) {
      return sum += counts[node];
    }, 0.0);

    var delta = 0.05
      , lower = 1.0 / 3 - 0.05
      , upper = 1.0 / 3 + 0.05;

    for (node in counts) {
      (counts[node] / total).should.be.within(lower, upper);
    }
  }
};
