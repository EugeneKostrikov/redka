'use strict';
const redisClient = require('./redis-client');

function Destructor(redisOptions){
  this.redis = redisClient.initialize(redisOptions);
}

Destructor.prototype.drain = function(job){
  this.redis.del(job.id, (err) => {
    if (err) throw err;
    let list = job.status === 'complete' ? job.queue + '_complete' : job.queue + '_failed';
    this.redis.lrem(list, -1, job.id, (err) => {
      if (err) throw err;
    });
  });
};

exports.initialize = function(redisOptions){
  return new Destructor(redisOptions);
};