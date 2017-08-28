'use strict';
const assert = require('assert');
const redisClient = require('./redis-client');
const fs = require('fs');
const path = require('path');
const scripts = [
  fs.readFileSync(path.join(__dirname, 'lua/delay-manager.lua')).toString(),
  fs.readFileSync(path.join(__dirname, 'lua/heartbeat-check.lua')).toString()
];

function DelayedJobsManager(connectionOptions, options){
  this.running = false;
  this.pollInterval = options.pollInterval || 100;
  this.client = redisClient.initialize(connectionOptions);
}

DelayedJobsManager.prototype.eval = function(sha){
  if (!this.running) return;
  this.client.evalsha(sha, 0, Date.now(), err => {
    if (err) throw err;
    setTimeout(() => this.eval(sha), this.pollInterval);
  });
};

DelayedJobsManager.prototype.start = function(){
  if (this.running) return;
  this.running = true;
  scripts.forEach(script => {
    this.client.script('load', script, (err, sha) => {
      if (err) throw err;
      this.eval(sha);
    });
  });
};

DelayedJobsManager.prototype.stop = function(callback){
  this.running = false;

  callback();
};

exports.create = function(connectionOptions, options){
  options = options || {};
  return new DelayedJobsManager(connectionOptions, options);
};