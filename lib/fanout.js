'use strict';
var debug = require('debug')('redka:fanout');
var async = require('async');

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

module.exports = function(redka, queueName, config){
  var worker = redka.worker(queueName);
  var names = Object.keys(config);
  var callbacks = names.reduce(function(memo, name){
    memo[name] = fanout(redka, config[name]);
    return memo;
  }, {});
  worker.register(callbacks);
};