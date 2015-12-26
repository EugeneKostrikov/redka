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
  this.status = 'INIT';
  this.lists = {
    pending: queueName + '_pending',
    progress: queueName + '_progress',
    complete: queueName + '_complete',
    failed: queueName + '_failed',
    callback: queueName + '_callback',
    backlog: queueName + '_backlog'
  };
  this.callbacks = {};
  this.pollingClient = client.initialize(clientOptions);
  this.client = client.initialize(clientOptions);
  if (options.timeout) {
    this.to = options.timeout;
    this._timeouts = {};
  }
}
util.inherits(Worker, events.EventEmitter);

Worker.prototype.register = function(callbacks){
  this.callbacks = _.extend(this.callbacks, callbacks);
  if (this.status === 'INIT') this.poll();
};


Worker.prototype.poll = function(){
  if (this.status === 'STOPPING') {
    this.status = 'STOPPED';
    return;
  }
  this.status = 'POLLING';
  this.dequeue((err, job) => {
    if (err) return this.handleError(err);
    if (!job && this.status === 'STOPPING'){
      this.status = 'STOPPED';
      return;
    }
    this.status = 'WORKING';
    this.work(job, (err, result) => {
      this.status = 'FINISHING';
      err ? this.fail(job, err) : this.complete(job, result);
    });
  });
};

Worker.prototype.stop = function(callback){
  this.status = 'STOPPING';
  let int = setInterval(() => {
    if (this.status === 'STOPPED'){
      clearInterval(int);
      callback();
    }
  }, 100);
};

Worker.prototype.dequeue = function(callback){
  if (this.status === 'STOPPING') return callback();
  this.pollingClient.brpoplpush(this.lists.pending, this.lists.progress, 1, (err, item) => {
    if (err) return callback(err);
    if (!item) return this.dequeue(callback);
    this.client.multi()
      .hset(item, 'dequeued', new Date().toISOString())
      .hgetall(item)
      .exec((err, results) => {
        if (err) return callback(err);
        var job = Job.create(results[1]);
        this.timeout(job);
        callback(null, job);
      });
  });
};

Worker.prototype.timeout = function(job){
  if (!this.to) return;
  debug('setting timeout on job', job.id);
  this._timeouts[job.id] = setTimeout(() => {
    this.fail(job, new Error('Worker timed out'), () => {
      this.emit('timeout', job);
    });
  }, this.to);
};

Worker.prototype.clearTimeout = function(job){
  if (this._timeouts && this._timeouts[job.id]){
    clearTimeout(this._timeouts[job.id]);
    delete this._timeouts[job.id];
  }
};

Worker.prototype.fail = function(job, err, callback){
  job.failed = new Date();
  job.error = err;
  job.stack = err.stack;

  debug('job failed', job);
  //TODO: run this atomically with lua script.
  //currently this limits to running single instance - why?
  this.client.lrange(this.lists.progress, 0, -1, (error, range) => {
    if (error) return this.handleError(error, callback);
    if (range.indexOf(job.id) === -1) return callback && callback();
    this.client.multi()
      .hmset(job.id, {
        status: 'failed',
        'failed': new Date().toISOString(),
        error: err,
        stack: err.stack
      })
      .lpush(this.lists.failed, job.id)
      .lpush(this.lists.callback, job.id)
      .lrem(this.lists.progress, 0, job.id)
      .exec((err) => {
        debug('moved job from progress to failed', job);
        this.clearTimeout(job);
        this.poll();
        err ? this.handleError(err, callback) : callback && callback();
      });
  });
};

Worker.prototype.complete = function(job, result, callback){
  job.complete = new Date();
  job.result = result;

  this.client.lrange(this.lists.progress, 0, -1, (err, range) => {
    if (err) return this.handleError(err, callback);
    if (range.indexOf(job.id) === -1) return callback && callback();
    this.client.multi()
      .hmset(job.id, {
        status: 'complete',
        complete: new Date().toISOString(),
        result: _.isObject(result) ? JSON.stringify(result) : result
      })
      .lpush(this.lists.complete, job.id)
      .lpush(this.lists.callback, job.id)
      .lrem(this.lists.progress, 0, job.id)
      .exec((err) => {
        debug('moved job from progress to complete', job);
        this.emit('complete', job);
        this.clearTimeout(job);
        this.poll();
        err ? this.handleError(err, callback) : callback && callback();
      });
  });
};

Worker.prototype.handleError = function(err, callback){
  if (callback) return callback(err);
  //TODO: handle error when there's no callback
};

Worker.prototype.throwToBacklog = function(job, callback){
  console.warn('throwToBacklog is deprecated');
  this.client.lrange(this.lists.progress, 0, -1, (err, range) => {
    if (err) return this.handleError(err, callback);
    if (range.indexOf(job.id) === -1) return callback();
    this.client.multi()
      .lpush(this.lists.backlog, job.id)
      .lrem(this.lists.progress, 0, job.id)
      .exec((err) => {
        this.clearTimeout(job);
        this.poll();
        err ? this.handleError(err, callback) : callback && callback();
      });
  });
};

Worker.prototype.work = function(job, callback){
  var cb = this.callbacks[job.name];
  //if (!cb) return this.throwToBacklog(job, callback);
  if (!cb) return this.fail(job, new Error('No callback registered for job'), callback);
  cb(job.params, (err, result) => {
    if (job.complete) return callback(new Error('Job callback is called twice'));
    job.complete = true;
    callback(err, result);
  });
};

exports.create = function(queue, redisOpts, workerOpts){
  return new Worker(queue, redisOpts, workerOpts);
};