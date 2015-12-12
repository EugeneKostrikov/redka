'use strict';
var util = require('util');
var events = require('events');
var debug = require('debug')('redka:reporter');
var Job = require('./job');
const redisClient = require('./redis-client');
const mongoClient = require('./mongo-client');

function Reporter(redisOptions, mongoOptions, options){
  this.redis = redisClient.initialize(redisOptions);
  this.options = options;
  this.lists = [];

  if (mongoOptions){
    mongoClient.connect(mongoOptions, (err, connection) => {
      if (err) throw err;
      this.mongo = connection;
      this.poll();
    });
  }else{
    this.poll();
  }
}
util.inherits(Reporter, events.EventEmitter);

Reporter.prototype.registerWorker = function(worker){
  this.lists.push(worker.lists.complete);
  this.lists.push(worker.lists.failed);
};

Reporter.prototype.poll = function(){
  if (this.lists.length === 0) return setTimeout(this.poll.bind(this), 1000);
  var args = this.lists.concat(500);
  this.redis.send_command("brpop", args, (err, item) => {
    if (err) this.handleError(err);
    item ? this.handle(item[1]) : this.poll();
  });
};

Reporter.prototype.handle = function(item){
  this.redis.hgetall(item, (err, data) => {
    if (err) return this.handleError(err);
    var job = Job.create(data);
    (this.mongo ? this.mongo.insert : this.noopinsert)(job.serialize(), (err) => {
      if (err) this.handleError(err);
      this.redis.del(item, (err) => {
        if (err) this.handleError(err);
        this.poll();
      });
    });
  });
};

Reporter.prototype.noopinsert = function(job, callback){
  callback();
};

Reporter.prototype.handleError = function(err){
  console.error('Redka reporter error', err);
};

exports.create = function(redis, mongo, options){
  return new Reporter(redis, mongo, options);
};

exports.dummy = function(redis){
  return new Reporter(redis);
};