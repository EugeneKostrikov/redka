'use strict';
const assert = require('assert');
const redisClient = require('./redis-client');
const fs = require('fs');
const path = require('path');
const script = fs.readFileSync(path.join(__dirname, 'delay-manager.lua')).toString();

function DelayedJobsManager(connectionOptions, options){
  this.pollInterval = options.pollInterval || 100;
  this.client = redisClient.initialize(connectionOptions);
}

DelayedJobsManager.prototype.start = function(){
  this.client.script('load', script, (err, sha) => {
    if (err) throw err;
    this.scriptId = sha;
    this.interval = setInterval(() => {
      this.client.evalsha(this.scriptId, 0, Date.now(), err => {
        if (err) throw err;
      });
    }, this.pollInterval);
  });
};

DelayedJobsManager.prototype.stop = function(callback){
  clearInterval(this.interval);
  callback();
};

exports.create = function(connectionOptions, options){
  options = options || {};
  return new DelayedJobsManager(connectionOptions, options);
};