'use strict';
const redis = require('redis').createClient();

redis.brpop('one', 1, function(err, item){
  console.log('first');
});

redis.brpop('one', 1, function(err, item){
  console.log('second');
});

process.nextTick(function(){
  console.log('tq ', redis.command_queue.length);

});
