'use strict';
var util = require('util');
var events = require('events');
var async = require('async');
var debug = require('debug')('redka:reporter');
var Job = require('./job');

function Reporter(redis, options){
  this.redis = redis;
  this.options = options;

  this.running = false;
  this.readyCallbacks = [];

  this.lists = [];
  this.poll();
}
util.inherits(Reporter, events.EventEmitter);

Reporter.prototype.registerWorker = function(worker){
  this.lists.push(worker.lists.complete);
  this.lists.push(worker.lists.failed);
};

Reporter.prototype.poll = function(){
  var that = this;
  that.onReady(function(){
    var args = that.lists.concat(500);
    that.redis.send_command("brpop", args, function (err, item) {
      if (err) console.error('polling error ', err);
      if (err) that.emit('error', err);
      item ? that.handle(item[1]) : that.poll();
    });
  });
};

Reporter.prototype.handle = function(item){
  var that = this;
  that.poll();
  that.redis.hgetall(item, function(err, data){
    if (err) return that.emit('error', err);
    var job = new Job(data);
    that.mongo.insert(job, function(err){
      if (err) that.emit('error', err);
      that.redis.del(item, function(err){
        if (err) that.emit('error', err);
      })
    });
  });
};

Reporter.prototype.setMongo = function(mongo){
  this.mongo = mongo;
  this.ready();
};

Reporter.prototype.onReady = function(callback){
  if (this.running) return process.nextTick(callback);
  this.readyCallbacks.push(callback);
};
Reporter.prototype.ready = function(){
  this.running = true;
  this.readyCallbacks.forEach(function(fn){fn()});
};


module.exports = Reporter;