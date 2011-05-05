var should = require('should')
  , hashring = require('../lib/hashring');

module.exports = {
  'Library version': function(){
     hashring.version.should.match(/^\d+\.\d+\.\d+$/);
  }
  
, 'Constructing with a string': function(){
    var ring = new hashring('192.168.0.102:11212');
    
    ring.nodes.should.have.length(1);
    ring.sortedKeys.length.should.be.above(1);
    Object.keys(ring.weights).should.have.length(0);
  }
  
, 'Constructing with a array': function(){
    var ring = new hashring(['192.168.0.102:11212', '192.168.0.103:11212', '192.168.0.104:11212']);
    
    ring.nodes.should.have.length(3);
    ring.sortedKeys.length.should.be.above(1);
    Object.keys(ring.weights).should.have.length(0);
  }
  
, 'Constructing with a object': function(){
    var ring = new hashring({'192.168.0.102:11212': 1, '192.168.0.103:11212': 2, '192.168.0.104:11212': 1});
    
    ring.nodes.should.have.length(3);
    ring.sortedKeys.length.should.be.above(1);
    Object.keys(ring.weights).should.have.length(3);
  }
  
, 'Constructing with a different algorithm': function(){
    var ring = new hashring('192.168.0.102:11212', 'md5');
    
    ring.nodes.should.have.length(1);
    ring.algorithm.should.equal('md5');
    ring.sortedKeys.length.should.be.above(1);
    Object.keys(ring.weights).should.have.length(0);
  }

, 'Looking up keys': function(){
    var ring = new hashring(['192.168.0.102:11212', '192.168.0.103:11212', '192.168.0.104:11212']);
    ring.nodes.indexOf(ring.getNode('foo')).should.be.above(-1);
    
    // NOTE we are going to do some flaky testing ;P
    ring.getNode('foo').should.equal('192.168.0.104:11212');
    ring.getNode('pewpew').should.equal('192.168.0.103:11212');
    
    // we are not gonna verify the results
    // we are just gonna test if we don't fuck something up in the code, so it throws errors or whatever
    
    // unicode keys, just because people roll like that
    ring.nodes.indexOf(ring.getNode('привет мир, Memcached и nodejs для победы')).should.be.above(-1);
    
    // other odd keys
    ring.nodes.indexOf(ring.getNode(1)).should.be.above(-1);
    ring.nodes.indexOf(ring.getNode(0)).should.be.above(-1);
    ring.nodes.indexOf(ring.getNode([])).should.be.above(-1);
    ring.nodes.indexOf(ring.getNode({wtf:'lol'})).should.be.above(-1);
    ring.getNode({wtf:'lol'}).should.equal(ring.getNode({wtf:'amazing .toStringing'}));
  }
  
, 'Replacing servers': function(){
    var ring = new hashring(['192.168.0.102:11212', '192.168.0.103:11212', '192.168.0.104:11212'])
      , amazon = ring.getNode('justdied')
      , skynet = '192.168.0.128:11212'
    
    ring.replaceServer(amazon, skynet);
    ring.cache.justdied.should.equal(skynet);
    ring.cache = {}; // clear cache
    ring.getNode('justdied').should.equal(skynet);
  }

, 'Removing servers': function(){
    var ring = new hashring(['192.168.0.102:11212', '192.168.0.103:11212', '192.168.0.104:11212']);
    ring.removeServer('192.168.0.102:11212');
    ring.nodes.indexOf('192.168.0.102:11212').should.equal(-1);
  }
  
  // kindly lended from `node-hash-ring` :)
, 'Distribution': function(){
    var iterations = 100000
      , nodes = {'192.168.0.102:11212': 1, '192.168.0.103:11212': 1, '192.168.0.104:11212': 1}
      , ring = new hashring(nodes)
      , genCode = function(length){
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
      node = ring.getNode(word);
      counts[node] = counts[node] || 0;
      counts[node]++;
    }
    
    var total = Object.keys(counts).reduce(function(sum, node) {
        return sum += counts[node];
      }, 0.0);
    
    var delta = 0.05
      , lower = 1.0 / 3 - 0.05
      , upper = 1.0 / 3 + 0.05;
      
    for(node in counts) {
      (counts[node] / total).should.be.within(lower, upper);
    }
  }
};