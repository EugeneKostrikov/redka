'use strict';
const sinon = require('sinon');

function stub(y){
  return y ? sinon.stub().yields() : sinon.stub().returnsThis();
}
function createRedisMethods(yields){
  return ['hmset', 'lpush', 'hset', 'hgetall', 'lrange', 'lrem', 'brpoplpush', 'send_command', 'del', 'script', 'evalsha'].reduce(function(m, k){
    m[k] = stub(yields);
    return m;
  }, {});
}
exports.mockRedis = function(){
  const multi = createRedisMethods(false);
  multi.exec = sinon.stub().yieldsAsync();
  const redis = createRedisMethods(true);
  redis.multi = sinon.stub().returns(multi);
  return redis;
};