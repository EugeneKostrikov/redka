'use strict';
var redis = require('redis');

exports.initialize = function(opts){
  console.log('connecting redis to ', opts, new Error().stack);
  return redis.createClient(opts.port, opts.host);
};