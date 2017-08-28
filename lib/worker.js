'use strict';
const _ = require('underscore');
const debug = require('debug')('redka:worker');
const events = require('events');
const util = require('util');
const client = require('./redis-client');
const Job = require('./job');
const JobContext = require('./job-context');
const JobEvents = require('./job-events');

function Worker(queueName, clientOptions, options, keysPrefix){
  options = options || {};
  this.queue = queueName;
  this.prefix = keysPrefix;
  this.status = 'INIT';
  this.lists = {
    pending: queueName + '_pending',
    progress: queueName + '_progress',
    backlog: queueName + '_backlog',
    delay: keysPrefix + '_global-delay'
  };
  this.callbacks = {};
  this.pollingClient = client.initialize(clientOptions);
  this.client = client.initialize(clientOptions);
  this.jobEventEmitter = JobEvents.createEmitter(clientOptions);

  if (options.timeout) {
    if (!_.isNumber(options.timeout))
      throw new Error('Expected options.timeout to be a number, got ' + typeof options.timeout);
    this.to = options.timeout;
    this._timeouts = {};
  }
}
util.inherits(Worker, events.EventEmitter);

Worker.prototype.register = function(callbacks){
  this.callbacks = _.extend(this.callbacks, callbacks);
  debug(`Worker will process jobs ${Object.keys(this.callbacks).join(', ')}`);
  if (this.status === 'INIT') {
    this.poll();
  }
};


Worker.prototype.poll = function(){
  debug(`.poll: entered polling loop with status ${this.status}`);
  if (this.status === 'STOPPING') {
    debug(`.poll: status is STOPPING. Changing to STOPPED`);
    this.status = 'STOPPED';
    return;
  }
  this.status = 'POLLING';
  debug(`.poll - Changed status to POLLING`);
  this.dequeue((err, job) => {
    debug(`.poll - Job dequeued. Error ${!!err}, job id ${job && job.id}`);
    if (err) return this.handleError(err);
    if (!job) return this.poll();
    this.status = 'WORKING';
    debug(`.poll - Changed status to WORKING`);
    const stopHeartbeat = this.heartbeat(job);
    this.work(job, (err, result) => {
      stopHeartbeat();
      debug(`.poll - Work complete ${job.id}`);
      switch (job.status){
        case 'complete':
          debug(`.poll - Job complete ${job.id}`);
          this.complete(job, result);
          break;
        case 'failed':
          debug(`.poll - Job failed ${job.id}`);
          this.fail(job, err);
          break;
        case 'retry':
          debug(`.poll - Retrying ${job.id}`);
          this.retry(job);
          break;
        default:
          debug(`.poll - Unexpected job state ${job.id}`);
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
  this.pollingClient.brpoplpush(this.lists.pending, this.lists.progress, 1, (err, item) => {
    if (err) return callback(err);
    if (!item) return this.dequeue(callback);
    this.client.multi()
      .hset(item, 'dequeued', new Date().toISOString())
      .hset(item, 'heartbeat', Date.now())
      .hgetall(item)
      .exec((err, results) => {
        if (err) return callback(err);
        const job = Job.create(results[2]);
        this.jobEventEmitter.jobDequeued(job);
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
  this.poll();
  job.failed = new Date();
  job.error = err.message || err;
  job.stack = err.stack;

  debug('job failed', job);
  this.client.lrange(this.lists.progress, 0, -1, (error, range) => { //yield full list
    if (error) return this.handleError(error, callback);
    if (range.indexOf(job.id) === -1) return callback && callback();
    this.client.multi()
      .del(job.id)
      .lrem(this.lists.progress, 0, job.id) //removes all equal to job.id
      .exec(err => {
        debug('moved job from progress to failed', job);
        this.jobEventEmitter.jobFailed(job);
        this.clearTimeout(job);
        err ? this.handleError(err, callback) : callback && callback();
      });
  });
};

Worker.prototype.complete = function(job, result, callback){
  this.poll();
  job.status = 'complete';
  job.complete = new Date();
  job.result = result;

  this.client.lrange(this.lists.progress, 0, -1, (err, range) => {
    if (err) return this.handleError(err, callback);
    if (range.indexOf(job.id) === -1) return callback && callback();
    this.client.multi()
      .del(job.id)
      .lrem(this.lists.progress, 0, job.id)
      .exec((err) => {
        debug('moved job from progress to complete', job);
        this.jobEventEmitter.jobComplete(job);
        this.clearTimeout(job);
        err ? this.handleError(err, callback) : callback && callback();
      });
  });
};

Worker.prototype.handleError = function(err, callback){
  if (callback) return callback(err);
  //TODO: handle error when there's no callback
};

Worker.prototype.retry = function(job){
  this.poll();
  job.attempt += 1;

  this.client.multi()
    .hmset(job.id, {
      delay: job.delay || 0,
      attempt: job.attempt,
      status: job.status || '',
      notes: JSON.stringify(job.notes) || '{}',
      heartbeat: ''
    })
    .lrem(this.lists.progress, 0, job.id)
    .lpush(this.lists.delay, job.id)
    .exec(err => {
      if (err) return this.fail(job, err);
      this.jobEventEmitter.jobRetry(job);
      this.clearTimeout(job);
    });
};

Worker.prototype.work = function(job, callback){
  const cb = this.callbacks[job.name];
  if (!cb) return this.fail(job, new Error(`No callback registered for job ${job.queue} ${job.name}`), callback);
  const jobContext = JobContext.create(job, callback);
  cb.call(jobContext, job.params, (err, result) => {
    if (job.status === 'complete' || job.status === 'failed') job.doubleCallback = true;
    job.status = err ? 'failed' : 'complete';
    debug('[work]: job complete ', job.status);
    callback(err, result);
  });
};

Worker.prototype.heartbeat = function(job){
  const interval = setInterval(() => {
    this.client.hset(job.id, 'heartbeat', Date.now(), err => {
      if (err) {
        console.error('Error heartbeating for job ', job.id);
        console.error(err);
      }
    });
  }, 1000);
  return () => clearInterval(interval);
};

exports.create = function(queue, redisOpts, workerOpts, keysPrefix){
  return new Worker(queue, redisOpts, workerOpts, keysPrefix);
};