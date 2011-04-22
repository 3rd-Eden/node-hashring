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
};