'use strict';
const _ = require('underscore');
const debug = require('debug')('redka:reporter');
const redisClient = require('./redis-client');
const mongoClient = require('./mongo-client');

function Reporter(redisOptions, mongoOptions, options){
  this.redis = redisClient.initialize(redisOptions);
  this.options = options;
  this.queue = [];

  if (mongoOptions){
    mongoClient.connect(mongoOptions, (err, connection) => {
      if (err) throw err;
      this.mongo = connection;
    });
  }else{
    this.mongo = {
      insertOne: function(j,c){c();}
    };
  }
}

Reporter.prototype.push = function(job){
  if (!this.mongo){
    this.queue.push(job.id);
    setTimeout(() => {
      this.queue = _.without(this.queue, job.id);
      this.push(job);
    }, 100);
  }else{
    this.mongo.insertOne(job.serialize(), (err) => {
      if (err) this.handleError(err);
    });
  }
};

Reporter.prototype.noopinsert = function(job, callback){
  callback();
};

Reporter.prototype.stop = function(callback){
  if (this.queue.length === 0) return callback();
  const int = setInterval(() => {
    if (this.queue.length === 0){
      clearInterval(int);
      callback();
    }
  }, 100);
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