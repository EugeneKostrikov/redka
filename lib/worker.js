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
  this.pollingClient = client.initialize(clientOptions);
  this.client = client.initialize(clientOptions);
  if (options.timeout) this.to = options.timeout;
}
util.inherits(Worker, events.EventEmitter);

Worker.prototype.register = function(callbacks){
 this.callbacks = _.extend(this.callbacks, callbacks);
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
  that.client.hmset(job.id, job.serialize(), function(err){
    if (err) return that.handleError(err, callback);
    that.client.lpush(that.lists.pending, job.id, function(err){
      if (err) return that.handleError(err, callback);
      callback && callback();
    });
  });
};

Worker.prototype.dequeue = function(callback){
  var that = this;
  that.pollingClient.brpoplpush(that.lists.pending, that.lists.progress, 10, function(err, item) {
    if (err) return callback(err);
    if (!item) return that.dequeue(callback);
    that.client.multi()
      .hset(item, 'dequeued', new Date().toISOString())
      .hgetall(item)
      .exec(function(err, results){
        if (err) return callback(err);
        var job = new Job(results[1]);
        debug('worker dequeued ', job);
        that.emit('dequeued', job);
        that.timeout(job);
        callback(null, job);
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
  that.once('failed', clearTime);
  that.once('complete', clearTime);
};

Worker.prototype.fail = function(job, err, callback){
  var that = this;
  job.failed = new Date();
  job.error = err;
  job.stack = err.stack;

  debug('job failed', job);
  //TODO: run this atomically with lua script.
  //currently this limits to running single instance
  that.client.lrange(that.lists.progress, 0, -1, function(error, range){
    if (error) return that.handleError(error, callback);
    if (range.indexOf(job.id) === -1) return callback && callback();
    that.client.multi()
      .hmset(job.id, {
        status: 'failed',
        'failed': new Date().toISOString(),
        error: err,
        stack: err.stack
      })
      .lpush(that.lists.failed, job.id)
      .lrem(that.lists.progress, 0, job.id)
      .exec(function(err, results){
        debug('moved job from progress to failed', job);
        err ? that.handleError(err, callback) : callback && callback();
        that.emit('failed', job);
        that.poll();
      });
  });
};

Worker.prototype.complete = function(job, result, callback){
  var that = this;
  job.complete = new Date();
  job.result = result;

  that.client.lrange(that.lists.progress, 0, -1, function(err, range){
    if (err) return that.handleError(err, callback);
    if (range.indexOf(job.id) === -1) return callback && callback();
    that.client.multi()
      .hmset(job.id, {
        status: 'complete',
        complete: new Date().toISOString(),
        result: _.isObject(result) ? JSON.stringify(result) : result
      })
      .lpush(that.lists.complete, job.id)
      .lrem(that.lists.progress, 0, job.id)
      .exec(function(err, results){
        debug('moved job from progress to complete', job);
        err ? that.handleError(err, callback) : callback && callback();
        that.emit('complete', job);
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
  cb(job.params, function(err, result){
    if (job.complete) return callback(new Error('Job callback is called twice'));
    job.complete = true;
    callback(err, result);
  });
};

module.exports = Worker;