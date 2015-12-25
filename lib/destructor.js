'use strict';
const redisClient = require('./redis-client');

function Destructor(redisOptions){
  this.redis = redisClient.initialize(redisOptions);
}

Destructor.prototype.drain = function(jobid){
  this.redis.del(jobid, (err) => {
    if (err) throw err;
  });
};

exports.initialize = function(redisOptions){
  return new Destructor(redisOptions);
};