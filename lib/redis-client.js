'use strict';
var redis = require('redis');

module.exports = function(opts){
  return redis.createClient(opts.port, opts.host);
};