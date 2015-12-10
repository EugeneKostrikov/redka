'use strict';
const async = require('async');
var debug = require('debug')('redka:main');
var mongo = require('./mongo-client');
var worker = require('./worker');
var Reporter = require('./reporter');
var fanout = require('./fanout');
var Job = require('./job');

function Redka(options){
  this.options = options;
  this.client = require('./redis-client').initialize(options.redis);
  this.subscribeClient = require('./redis-client').initialize(options.redis);
  this._workers = {};
  this.running = false;
  this.prefix = options.prefix || 'redka_';
  this.readyCallbacks = [];

  if (options.enableReporting){ //skipping this part for now
    if (!options.mongodb) throw new Error('MongoDB options are required for message bus monitoring');
    this.reporter = Reporter.create(options.redis, options.mongodb, options.reportingOptions);
    this.ready();
  }else{
    this.reporter = Reporter.dummy(options.redis);
    this.ready();
  }
}

Redka.prototype.onReady = function(callback){
  if (this.running) return process.nextTick(callback);
  this.readyCallbacks.push(callback);
};

Redka.prototype.getPubsubChannel = function(){
  var that = this;
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

Redka.prototype.worker = function(queue, options){
  var queueName = this.prefix + queue;
  if (!!this._workers[queueName]) throw new Error('Cannot create duplicate worker. Queue ' + queue + ' is busy');
  var w = worker.create(queueName, this.options.redis, options);
  this._workers[queueName] = w;
  if (this.options.enableReporting) this.reporter.registerWorker(w);
  return w;
};

Redka.prototype.removeWorker = function(queue, callback){
  let queueName = this.prefix + queue;
  if (!this._workers[queueName]) return callback(new Error(`Cannot remove worker ${queue} - no worker registered`));
  this._workers[queueName].stop(callback);
};

Redka.prototype.enqueue = function(queue, name, params, callback){
  var that = this;
  var queueName = this.prefix + queue;
  var job = Job.create(queueName, name, params);
  that.client.hmset(job.id, job.serialize(), function(err){
    if (err) return callback && callback(err);
    that.client.lpush(queueName + '_pending', job.id, function(err){
      if (err) return callback && callback(err);
      debug('worker enqueued ', job);
      callback && callback();
    });
  });
};

Redka.prototype.start = function(){
  console.warn('Redka.start is deprecated. Workers start as soon as they are added');
};

Redka.prototype.stop = function(callback){
  async.each(Object.keys(this._workers), (worker, next) => {
    let q = worker.replace(new RegExp('^' + this.prefix), '');
    this.removeWorker(q, next);
  }, callback);
};

Redka.prototype.ready = function(){
  this.readyCallbacks.forEach(function(cb){cb()});
  this.running = true;
};

Redka.prototype.fanout = function(sourceQueue, config){
  fanout.init(this, sourceQueue, config);
};

Redka.prototype.status = function(callback){
  var results = {};
  var that = this;
  var requested = 0, complete = 0;
  Object.keys(that._workers).forEach(function(k){
    var w = that._workers[k];
    requested++;
    that.client.multi()
      .llen(w.lists.pending)
      .llen(w.lists.progress)
      .llen(w.lists.complete)
      .llen(w.lists.failed)
      .exec(function(err, res){
        if (err) return callback(err);
        results[w.queue.replace(that.prefix, '')] = {
          pending: res[0],
          progress: res[1],
          complete: res[2],
          failed: res[3]
        };
        complete++;
        if (requested === complete) callback(null, results);
      });
  });
};

Redka.prototype._reset = function(callback){
  var keys = Object.keys(this._workers);
  var that = this;
  var count = 0;
  keys.forEach(function(key){
    var worker = that._workers[key];
    worker.stats = {
      enqueued: 0,
      dequeued: 0,
      failed: 0,
      complete: 0
    };
    that.client.multi()
      .del(worker.lists.pending)
      .del(worker.lists.progress)
      .del(worker.lists.complete)
      .del(worker.lists.failed)
      .exec(function(err){
        if (err) return callback(err);
        count++;
        if (count === keys.length) callback();
      });
  });
};

module.exports = Redka;