"use strict";
const _ = require('lodash');
const ObjectID = require('mongodb').ObjectID;
const MongoClient = require('mongodb').MongoClient;
const JobEvents = require('../../lib/job-events');
const debug = require('debug')('redka:plugins:mongo-reporter');

function Reporter(options){
  this.options = {
    redis: options.redis,
    mongo: options.mongodb
  };

  this.writeQueue = [];

}

Reporter.prototype.start = function(){
  debug('starting mongo reporting plugin');
  this.receiver = JobEvents.createReceiver(this.options.redis);
  this.receiver.onEnqueued(this.handleEnqueued.bind(this));
  this.receiver.onDequeued(this.handleDequeued.bind(this));
  this.receiver.onRetry(this.handleRetry.bind(this));
  this.receiver.onComplete(this.handleComplete.bind(this));
  this.receiver.onFailed(this.handleFailed.bind(this));

  MongoClient.connect(this.options.mongo, (err, db) => {
    if (err) throw err;
    this.collection = db.collection('redka_jobs');
    this.write();
    this.started = true;
  });
};

Reporter.prototype.stop = function(callback){
  if (!this.started) return callback();
  const int = setInterval(() => {
    if (this.writeQueue.length === 0) {
      clearInterval(int);
      callback();
    }
  }, 100);
};


Reporter.prototype.write = function(){
  debug('trying to write queue', this.writeQueue.length);
  const nextUpdate = this.writeQueue.shift();
  if (!nextUpdate) return setTimeout(() => this.write(), 1000);

  //handle dupes
  this.collection.update(nextUpdate.query, nextUpdate.update, {upsert: true}, err => {
    if (err && err.code === 11000) return this.write(); //picked up by two instances. Safe to ignore
    if (err) throw err;
    this.write();
  });
};

Reporter.prototype.escapeKeys = function(object){
  if (!_.isPlainObject(object) && !_.isArray(object)) return object;
  return _.transform(object, (memo, value, key) => {
    const undotted = _.isString(key) ? key.replace(/\./g, '->') : key;
    if (_.isString(undotted) && undotted.charAt(0) === '$'){
      memo[undotted.replace(/^\$/, '_')] = _.isObject(value) ? this.escapeKeys(value) : value;
    }else{
      memo[undotted] = _.isObject(value) ? this.escapeKeys(value) : value;
    }
    return memo;
  }, _.isArray(object) ? [] : {});
};

Reporter.prototype.addToWriteQueue = function(updateConfig){
  this.writeQueue.push(updateConfig);
};

Reporter.prototype.handleEnqueued = function(event){
  debug('Enqueued event ', event.job.id);
  this.addToWriteQueue({
    query: {_id: new ObjectID(event.job.id)},
    update: {
      $set: this.escapeKeys(_.pick(event.job, 'queue', 'name', 'params', 'delay', 'attempt', 'notes', 'enqueued', 'delay', 'wasOnceStuck'))
    }
  });
};

Reporter.prototype.handleDequeued = function(event){
  debug('Dequeued event ', event.job.id);
  this.addToWriteQueue({
    query: {_id: new ObjectID(event.job.id)},
    update: {
      $set: this.escapeKeys(_.pick(event.job, 'status', 'dequeued', 'wasOnceStuck'))
    }
  });
};

Reporter.prototype.handleRetry = function(event){
  debug('Retry event ', event.job.id);
  this.addToWriteQueue({
    query: {_id : new ObjectID(event.job.id)},
    update: {
      $set: this.escapeKeys(_.pick(event.job, 'status', 'attempt', 'notes', 'wasOnceStuck'))
    }
  });
};

Reporter.prototype.handleComplete = function(event){
  debug('Complete event ', event.job.id);
  this.addToWriteQueue({
    query: {_id: new ObjectID(event.job.id)},
    update: {
      $set: this.escapeKeys(_.pick(event.job, 'status', 'complete', 'result', 'notes', 'wasOnceStuck'))
    }
  });
};

Reporter.prototype.handleFailed = function(event){
  debug('Failed event ', event.job.id);
  this.addToWriteQueue({
    query: {_id: new ObjectID(event.job.id)},
    update: {
      $set: this.escapeKeys(_.pick(event.job, 'status', 'failed', 'error', 'stack', 'notes', 'wasOnceStuck'))
    }
  });
};

exports.createReporter = function(options){
  return new Reporter(options);
};

