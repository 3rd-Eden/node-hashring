## hashring

Hash ring provides consistent hashing based on the `libketema` library.

### Installation

You can either install it using the Node Package Manager (NPM)

    npm install hashring

Or fork this repository to your machine

    git clone git://github.com/3rd-Eden/node-hashring.git hashring

### Basic usage

The constructor is designed to handle multiple arguments types as the hash ring can be used for different use cases. You have the ability to use a `String` to add a single server, a `Array` to provide multiple servers or an `Object` to provide servers with a custom weight. The weight can be used to give a server a bigger distribution in the hash ring. For example you have 3 machines, 2 of those machines have 8 gig memory and one has 32 gig of memory because the last server has more memory you might it to handle more keys than the other server. So you can give it a weight of 2 and the other servers a weight of 1.

Creating a hash ring with only one server

``` javascript
var hashring = require('hashring');
var ring = new hashring('192.168.0.102:11212')
```

Creating a hash ring with multiple servers

``` javascript
var hashring = require('hashring');
var ring = new hashring([ '192.168.0.102:11212', '192.168.0.103:11212', '192.168.0.104:11212']);
```

Creating a hash ring with multiple servers and weights

``` javascript
var hashring = require('hashring');
var ring = new hashring({
  '192.168.0.102:11212': 1
, '192.168.0.103:11212': 2
, '192.168.0.104:11212': 1
});
```