'use strict';
var Worker = require('./worker');
var fanout = require('./fanout');
var Job = require('./job');

function Redka(options){
  this.options = options;
  this.client = require('./redis-client')(options.redis);
  this._workers = {};
  this.prefix = options.prefix || 'redka_';

  this.completeCallbacks = [];
}

Redka.prototype.worker = function(queue){
  var queueName = this.prefix + queue;
  if (!!this._workers[queueName]) throw new Error('Cannot create duplicate worker. Queue ' + queue + ' is busy');
  var w = new Worker(queueName, this.options.redis);
  this._workers[queueName] = w;
  return w;
};

Redka.prototype.enqueue = function(queue, name, params, callback){
  var queueName = this.prefix + queue;
  var worker = this._workers[queueName];
  if (!worker) throw new Error('Cannot enqueue job, queue ' + queueName + ' does not exist');
  //this._workers[queueName].enqueue(name, params, callback);
  var that = this;
  var job = new Job(queueName, name, params);
  that.client.lpush(worker.lists.pending, job.serialize(), function(err){
    if (err) return callback && callback(err, callback);
    worker.stats.enqueued++;
    worker.emit('enqueued', {});
    callback && callback();
  });
};

Redka.prototype.start = function(){
  var keys = Object.keys(this._workers);
  var that = this;
  keys.forEach(function(key){
    that._workers[key]._start();
  });
};

Redka.prototype.stop = function(){

};

Redka.prototype.fanout = function(sourceQueue, config){
  fanout(this, sourceQueue, config);
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