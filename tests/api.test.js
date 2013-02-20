"use strict";

var Hashring = require('../');

describe('HashRing', function () {
  it('exposes the library version', function () {
    Hashring.version.should.match(/^\d+\.\d+\.\d+$/);
  });

  describe('API', function () {
    it('constructs with a string', function () {
      var ring = new Hashring('192.168.0.102:11212');

      ring.servers.should.have.length(1);
      ring.ring.length.should.be.above(1);
    });

    it('constructs with a array', function () {
      var ring = new Hashring([
          '192.168.0.102:11212'
        , '192.168.0.103:11212'
        , '192.168.0.104:11212'
      ]);

      ring.servers.should.have.length(3);
      ring.ring.length.should.be.above(1);
    });

    it('constructs with a object', function () {
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
    });

    it('constructs with a object with per node vnodes', function () {
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
    });

    it('constructs with no arguments', function () {
      var ring = new Hashring();

      ring.servers.should.have.length(0);
      ring.ring.should.have.length(0);
    });

    it('accepts different algorithms', function () {
      var ring = new Hashring('192.168.0.102:11212', 'sha1');

      ring.servers.should.have.length(1);
      ring.algorithm.should.equal('sha1');
      ring.ring.length.should.be.above(1);
    });

    describe("#add", function () {
      it('adds server after zero-argument constructor', function () {
        var ring = new Hashring();
        ring.add('192.168.0.102:11212');

        ring.servers.should.have.length(1);
        ring.ring.length.should.be.above(1);
      });
    });

    describe('#get', function () {
      it('looks up keys', function () {
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
      });
    });

    describe('#swap', function () {
      it('swaps servers', function () {
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
      });
    });

    describe('#remove', function () {
      it('removes servers', function () {
        var ring = new Hashring([
            '192.168.0.102:11212'
          , '192.168.0.103:11212'
          , '192.168.0.104:11212'
        ]);

        ring.remove('192.168.0.102:11212');
        ring.ring.forEach(function (node) {
          node.server.should.not.equal('192.168.0.102:11212');
        });
      });

      it('Removes the last server', function () {
        var ring = new Hashring('192.168.0.102:11212');
        ring.remove('192.168.0.102:11212');

        ring.servers.should.have.length(0);
        ring.ring.should.have.length(0);
      });
    });
  });
});
