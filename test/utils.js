'use strict';

exports.redis = require('../lib/redis-client').initialize({
  port: 6379,
  host: '127.0.0.1'
});