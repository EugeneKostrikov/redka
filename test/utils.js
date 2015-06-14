'use strict';

exports.redis = require('../lib/redis-client').initialize({
  host: process.env.REDIS_PORT_6379_TCP_ADDR || '127.0.0.1',
  port: process.env.REDIS_PORT_6379_TCP_PORT || 6379
});