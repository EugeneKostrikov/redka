'use strict';
const redisClient = require('./redis-client');
const debug = require('debug')('redka:destructor');

function Destructor(redisOptions){
  this.redis = redisClient.initialize(redisOptions);
}

Destructor.prototype.drain = function(job){
  debug('destroying job data ', job.id);
  this.redis.del(job.id, (err, ndropped) => {
    if (err) throw err;
    debug('job key dropped ', err, ndropped);
    let list = job.status === 'complete' ? job.queue + '_complete' : job.queue + '_failed';
    debug('removing job from list ', list, job.id);
    this.redis.lrem(list, -1, job.id, (err, ndropped) => {
      debug('removed job from the list ', err, ndropped);
      if (err) throw err;
    });
  });
};

exports.initialize = function(redisOptions){
  return new Destructor(redisOptions);
};