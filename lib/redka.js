'use strict';
const async = require('async');
const _ = require('underscore');
const assert = require('assert');
const debug = require('debug')('redka:main');
const worker = require('./worker');
const fanout = require('./fanout');
const Job = require('./job');
const DelayedJobsManager = require('./delayed-jobs-manager');
const BatchProcessor = require('./batch-processor');
const Callbacks = require('./callbacks');
const WorkerMultiplex = require('./worker-multiplex');
const JobEventEmitter = require('./job-events');

function Redka(options = {}){
  this.options = options;
  this.client = require('./redis-client').initialize(options.redis);
  this.subscribeClient = require('./redis-client').initialize(options.redis);
  this._workers = {};
  this.running = false;
  this.prefix = options.prefix || 'redka_';
  this.readyCallbacks = [];

  this.jobCallbacks = Callbacks.initialize(options.redis);
  this.jobEventEmitter = JobEventEmitter.createEmitter(options.redis);
  this.jobEventReceiver = JobEventEmitter.createReceiver(options.redis);

  this.batchProcessor = BatchProcessor.makeBatcher(options.redis, {
    prefix: this.prefix
  });
  this.batchProcessor.start();

  if (options.runDelayedJobsManager){
    this.delayedJobsManager = new DelayedJobsManager.create(options.redis, options.delayOptions);
    this.delayedJobsManager.start();
  }

  this.ready();
}

Redka.prototype.onReady = function(callback){
  if (this.running) return process.nextTick(callback);
  this.readyCallbacks.push(callback);
};

Redka.prototype.getPubsubChannel = function(){
  const that = this;
  return {
    publish: function(){
      that.client.publish.apply(that.client, arguments);
    },
    subscribe: function(){
      that.subscribeClient.subscribe.apply(that.subscribeClient, arguments);
    },
    on: function(){
      that.subscribeClient.on.apply(that.subscribeClient, arguments);
    },
    once: function(){
      that.subscribeClient.once.apply(that.subscribeClient, arguments);
    }
  }
};

Redka.prototype.worker = function(queue, options = {}){
  const queueName = this.prefix + queue;
  if (!!this._workers[queueName]) throw new Error('Cannot create duplicate worker. Queue ' + queue + ' is busy');
  const multiplex = WorkerMultiplex.create();
  for (let i = 0; i < (options.parallel || 1); i++){
    multiplex._addWorker(
      worker.create(queueName, this.options.redis, options, this.prefix)
    );
  }
  this._workers[queueName] = multiplex._workers;

  return multiplex;
};

Redka.prototype.removeWorker = function(queue, callback){
  let queueName = this.prefix + queue;
  if (!this._workers[queueName]) return callback(new Error(`Cannot remove worker ${queue} - no worker registered`));
  async.each(this._workers[queueName], (worker, next) => {
    worker.stop(next);
  }, err => {
    if (err) return callback(err);
    this._workers[queueName] = undefined;
    callback();
  });
};

Redka.prototype.enqueue = function(queue, name, params, options, callback){
  if (_.isFunction(options)) {
    callback = options;
    options = {};
  }
  assert(queue !== '_global-delay', '_global-delay is redka reserved queue name');
  assert(queue !== '_batcher', '__batcher is redka reserver queue name');

  const queueName = this.prefix + queue;
  const job = Job.create(queueName, name, params, options);
  debug(`Created job ${job.id}`);

  const unsub = [];

  const jobCompleteCallback = (event) => {
    if (event.job.id !== job.id) return;
    unsub.forEach(fn => fn());
    callback && callback(null, event.job.result);
  };

  const jobFailedCallback = (event) => {
    if (event.job.id !== job.id) return;
    unsub.forEach(fn => fn());
    const error = new Error(event.job.error);
    error.stack = event.job.stack;
    callback && callback(error, null);
  };

  unsub.push(this.jobEventReceiver.onComplete(jobCompleteCallback));
  unsub.push(this.jobEventReceiver.onFailed(jobFailedCallback));

  this.client.hmset(job.id, job.serialize(), (err) => {
    if (err) return callback && callback(err);

    ((job, callback) => {
      if (_.isNumber(job.delay) && job.delay !== 0){
        this.client.lpush(this.prefix + '_global-delay', job.id, callback);
      }else{
        this.client.lpush(queueName + '_pending', job.id, callback)
      }
    })(job, err => {
      if (err) return callback && callback(err);
      this.jobEventEmitter.jobEnqueued(job);
      debug('enqueued ', job.id);
    });
  });
};

Redka.prototype.stop = function(callback){
  async.parallel([
    callback => {
      callback();
    },
    callback => {
      debug(`Removing workers ${Object.keys(this._workers).join(', ')}`);
      async.each(Object.keys(this._workers), (queueName, nextWorkerKey) => {
        async.each(this._workers[queueName], (worker, nextWorker) => {
          let q = queueName.replace(new RegExp('^' + this.prefix), '');
          this.removeWorker(q, nextWorker);
        }, nextWorkerKey);
      }, function(err){
        debug('workers stopped ', err);
        callback();
      });
    },
    callback => {
      this.delayedJobsManager.stop(function(err){
        debug('delayer stopped ', err);
        callback();
      });
    }
  ], callback);
};

Redka.prototype.ready = function(){
  this.readyCallbacks.forEach(function(cb){cb()});
  this.running = true;
};

Redka.prototype.fanout = function(sourceQueue, config){
  fanout.init(this, sourceQueue, config);
};

Redka.prototype.batch = function(config){
  this.batchProcessor.add(config);
};

module.exports = Redka;