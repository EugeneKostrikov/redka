'use strict';
var sinon = require('sinon');

function stub(y){
  return y ? sinon.stub().yieldsAsync() : sinon.stub().returnsThis();
}
function createRedisMethods(yields){
  return ['hmset', 'lpush', 'hset', 'hgetall', 'lrange', 'lrem', 'brpoplpush'].reduce(function(m, k){
    m[k] = stub(yields);
    return m;
  }, {});
}
exports.mockRedis = function(){
  var multi = createRedisMethods(false);
  multi.exec = sinon.stub().yieldsAsync();
  var redis = createRedisMethods(true);
  redis.multi = sinon.stub().returns(multi);
  return redis;
};