'use strict';
const assert = require('assert');
const redisClient = require('./redis-client');
const fs = require('fs');
const path = require('path');
const script = fs.readFileSync(path.join(__dirname, 'delay-manager.lua')).toString();

function DelayedJobsManager(connectionOptions, options){
  this.running = false;
  this.pollInterval = options.pollInterval || 100;
  this.client = redisClient.initialize(connectionOptions);
}

DelayedJobsManager.prototype.start = function(){
  if (this.running) return;
  this.running = true;
  this.client.script('load', script, (err, sha) => {
    if (err) throw err;
    this.scriptId = sha;
    this.interval = setInterval(() => {
      let cutoff = Date.now();
      this.client.evalsha(this.scriptId, 0, cutoff, err => {
        if (err) throw err;
      });
    }, this.pollInterval);
  });
};

DelayedJobsManager.prototype.stop = function(callback){
  clearInterval(this.interval);
  this.running = false;
  callback();
};

exports.create = function(connectionOptions, options){
  options = options || {};
  return new DelayedJobsManager(connectionOptions, options);
};