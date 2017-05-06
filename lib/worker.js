'use strict';
const _ = require('underscore');
const debug = require('debug')('redka:worker');
const events = require('events');
const util = require('util');
const client = require('./redis-client');
const Job = require('./job');
const JobContext = require('./job-context');

function Worker(queueName, clientOptions, options, keysPrefix){
  options = options || {};
  this.queue = queueName;
  this.prefix = keysPrefix;
  this.status = 'INIT';
  this.lists = {
    pending: queueName + '_pending',
    progress: queueName + '_progress',
    complete: queueName + '_complete',
    failed: queueName + '_failed',
    backlog: queueName + '_backlog',
    delay: keysPrefix + '_global-delay'
  };
  this.callbacks = {};
  this.pollingClient = client.initialize(clientOptions);
  this.client = client.initialize(clientOptions);
  if (options.timeout) {
    if (!_.isNumber(options.timeout))
      throw new Error('Expected options.timeout to be a number, got ' + typeof options.timeout);
    this.to = options.timeout;
    this._timeouts = {};
  }

  //Parallel jobs handling
  this.parallel = 1;
  //defines how quickly parallel worker will react to increased backlog
  this.spinup = options.spinup || 500;
  this.runningCount = 0;
  this.pendingDequeue = false;
  if (options.parallel) {
    if (!_.isNumber(options.parallel))
      throw new Error('Expected options.parallel to be a number, got ' + typeof options.parallel);
    this.parallel = options.parallel;
  }
}
util.inherits(Worker, events.EventEmitter);

Worker.prototype.register = function(callbacks){
  this.callbacks = _.extend(this.callbacks, callbacks);
  if (this.status === 'INIT') {
    for (let i = 0; i < this.parallel; i++){
      this.poll();
    }
  }
};


Worker.prototype.poll = function(){
  if (this.status === 'STOPPING') {
    if (this.runningCount === 0) this.status = 'STOPPED';
    return;
  }
  this.status = 'POLLING';
  this.dequeue((err, job) => {
    if (err) return this.handleError(err);
    if (!job) return this.poll();
    this.runningCount++;
    if (this.status === 'POLLING') this.status = 'WORKING';
    this.work(job, (err, result) => {
      this.runningCount--;
      switch (job.status){
        case 'retry':
          this.retry(job);
          break;
        case 'complete':
          this.complete(job, result);
          break;
        case 'failed':
          this.fail(job, err);
          break;
        default:
          this.fail(job, err || new Error('Unexpected job state'));
      }
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
  if (this.pendingDequeue) return setTimeout(() => this.dequeue(callback), this.spinup);
  this.pendingDequeue = true;
  this.pollingClient.brpoplpush(this.lists.pending, this.lists.progress, 1, (err, item) => {
    this.pendingDequeue = false;
    if (err) return callback(err);
    if (!item) return this.dequeue(callback);
    this.client.multi()
      .hset(item, 'dequeued', new Date().toISOString())
      .hgetall(item)
      .exec((err, results) => {
        if (err) return callback(err);
        const job = Job.create(results[1]);
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
  this.client.lrange(this.lists.progress, 0, -1, (error, range) => { //yield full list
    if (error) return this.handleError(error, callback);
    if (range.indexOf(job.id) === -1) return callback && callback();
    this.client.multi()
      .hmset(job.id, {
        status: 'failed',
        'failed': new Date().toISOString(),
        error: err || '',
        stack: err.stack || '',
        notes: JSON.stringify(job.notes) || '{}'
      })
      .lpush(this.lists.failed, job.id)
      .lpush(job.id + '_callback', job.id)
      .lrem(this.lists.progress, 0, job.id) //removes all equal to job.id
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
        result: _.isObject(result) ? JSON.stringify(result) : (result || ''),
        notes: JSON.stringify(job.notes) || '{}'
      })
      .lpush(this.lists.complete, job.id)
      .lpush(job.id + '_callback', job.id)
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

Worker.prototype.retry = function(job){
  this.client.multi()
    .hmset(job.id, {
      delay: job.delay || 0,
      attempt: job.attempt + 1,
      status: job.status || '',
      notes: JSON.stringify(job.notes) || '{}'
    })
    .lrem(this.lists.progress, 0, job.id)
    .lpush(this.lists.delay, job.id)
    .exec(err => {
      if (err) return this.fail(job, err);
      this.emit('retry', job);
      this.clearTimeout(job);
      this.poll();
    });
};

Worker.prototype.work = function(job, callback){
  const cb = this.callbacks[job.name];
  //if (!cb) return this.throwToBacklog(job, callback);
  if (!cb) return this.fail(job, new Error(`No callback registered for job ${job.queue} ${job.name}`), callback);
  const jobContext = JobContext.create(job, callback);
  cb.call(jobContext, job.params, (err, result) => {
    if (job.status === 'complete' || job.status === 'failed') return; //Job callback is called twice
    job.status = err ? 'failed' : 'complete';
    callback(err, result);
  });
};

exports.create = function(queue, redisOpts, workerOpts, keysPrefix){
  return new Worker(queue, redisOpts, workerOpts, keysPrefix);
};