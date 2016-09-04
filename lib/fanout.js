'use strict';
const debug = require('debug')('redka:fanout');
const async = require('async');

function fanout(redka, destinations){
  return function(data, callback){
    async.each(destinations, function(dest, next){
      debug('fanout cloning job ', data, ' to ', dest.queue, ' ', dest.name);
      redka.enqueue(dest.queue, dest.name, data);
      next();
    }, function(err){
      callback(err);
    });
  }
}

exports.fanout = fanout;

exports.init = function(redka, queueName, config){
  const worker = redka.worker(queueName);
  const names = Object.keys(config);
  const callbacks = names.reduce(function(memo, name){
    memo[name] = fanout(redka, config[name]);
    return memo;
  }, {});
  worker.register(callbacks);
};