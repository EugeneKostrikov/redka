'use strict';

exports.redis = require('../lib/redis-client')({
  port: 6379,
  host: '127.0.0.1'
});