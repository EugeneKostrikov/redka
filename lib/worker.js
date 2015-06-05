'use strict';
var _ = require('underscore');
var debug = require('debug')('redka:worker');
var events = require('events');
var util = require('util');
var client = require('./redis-client');
var Job = require('./job');

function Worker(queueName, clientOptions, options){
  options = options || {};
  this.queue = queueName;
  this.lists = {
    pending: queueName + '_pending',
    progress: queueName + '_progress',
    complete: queueName + '_complete',
    failed: queueName + '_failed'
  };
  this.client = client(clientOptions);
  if (options.timeout) this.to = options.timeout;
}
util.inherits(Worker, events.EventEmitter);

Worker.prototype.register = function(callbacks){
 this.callbacks = callbacks;
};

Worker.prototype._start = function(){
  this.poll();
};

Worker.prototype.poll = function(){
  var that = this;
  that.dequeue(function(err, job){
    if (err) return that.handleError(err);
    that.work(job, function(err, result){
      err ? that.fail(job, err) : that.complete(job, result);
    });
  });
};

Worker.prototype.enqueue = function(name, params, callback){
  var that = this;
  var job = new Job(this.queue, name, params);
  that.client.hmset(job.id, job, function(err){
    if (err) return that.handleError(err, callback);
    that.client.lpush(that.lists.pending, job.id, function(err){
      if (err) return that.handleError(err, callback);
      callback && callback();
    });
  });
};

Worker.prototype.dequeue = function(callback){
  var that = this;
  that.client.brpoplpush(that.lists.pending, that.lists.progress, 10, function(err, item) {
    if (err) return callback(err);
    if (!item) return that.poll();
    that.client.hgetall(item, function(err, data){
      if (err) return callback(err);
      var job = new Job(data);
      job.dequeued = new Date();
      that.client.hset(job.id, 'dequeued', new Date().toISOString(), function(err){
        if (err) return callback(err);
        debug('worker dequeued ', job);
        that.emit('dequeued', job);
        that.timeout(job);
        callback(null, job);
      });
    });
  });
};

Worker.prototype.timeout = function(job){
  var that = this;
  if (!that.to) return;
  function timeoutJob(){
    that.fail(job, new Error('Worker timed out'), function(){
      that.emit('timeout', job);
    });
  }
  debug('setting timeout on job', job.id);
  var t = setTimeout(timeoutJob, that.to);
  function clearTime(){
    debug('clearing timeout for job ', job.id);
    clearTimeout(t);
  }
  that.once('fail', clearTime);
  that.once('complete', clearTime);
};

Worker.prototype.fail = function(job, err, callback){
  var that = this;
  job.failed = new Date();
  job.error = err;
  that.emit('failed', job);
  debug('job failed', job);
  that.client.hmset(job.id, {
    'failed': new Date().toISOString(),
    error: _.isObject(err) ? JSON.stringify(err) : err
  }, function(err){
    if (err) return that.handleError(err, callback);
    that.client.rpoplpush(that.lists.progress, that.lists.failed, function(err, item){
      err ? that.handleError(err, callback) : callback && callback();
      that.poll();
    });
  });
};

Worker.prototype.complete = function(job, result, callback){
  var that = this;
  job.complete = new Date();
  job.result = result;
  that.emit('complete', job);
  that.client.hmset(job.id, {
    complete: new Date().toISOString(),
    result: _.isObject(result) ? JSON.stringify(result) : result
  }, function(err){
    if (err) return that.handleError(err, callback);
    debug('job complete', job);
    that.client.rpoplpush(that.lists.progress, that.lists.complete, function(err, item){
      err ? that.handleError(err, callback) : callback && callback();
      that.poll();
    });
  });
};

Worker.prototype.handleError = function(err, callback){
  if (callback) return callback(err);
};

Worker.prototype.work = function(job, callback){
  var cb = this.callbacks[job.name];
  if (!cb) return callback(new Error('No callback registered for job ' + job.name + ' on queue ' + job.queue));
  cb(job.params, callback);
};

module.exports = Worker;