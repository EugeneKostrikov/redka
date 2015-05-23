'use strict';

function fanout(redka, destinations){
  return function(data, callback){
    var count = 0;
    destinations.forEach(function(dest){
      redka.enqueue(dest.queue, dest.name, data, function(err){
        if (err) return callback(err);
        count++;
        if (count === destinations.length) callback();
      });
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