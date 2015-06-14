'use strict';
var redis = require('redis');

exports.initialize = function(opts){
  return redis.createClient(opts.port, opts.host);
};