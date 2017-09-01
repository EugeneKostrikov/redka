'use strict';
const assert = require('assert');
const ObjectId = require('mongodb').ObjectID;
const async = require('async');
const redisClient = require('./redis-client');
const fs = require('fs');
const path = require('path');
const batcherScript = fs.readFileSync(path.join(__dirname, 'lua/batch-processor.lua')).toString()

function Batcher(connectionOptions, options){
  this.running = false;
  this.batches = [];
  this.prefix = options.prefix || 'redka_';
  this.client = redisClient.initialize(connectionOptions);

}

Batcher.prototype.start = function(){
  this.running = true;
  this.client.script('load', batcherScript, (err, sha) => {
    if (err) throw err;
    this.eval(sha);
  });
};

Batcher.prototype.add = function(config){
  assert(config.sourceQueue, 'sourceQueue option is required');
  assert(config.sourceName, 'sourceName option is required');
  assert(config.targetQueue, 'targetQueue option is required');
  assert(config.targetName, 'targetName option is required');
  assert(config.batchSize, 'batchSize option is required');
  config.batchInterval = config.batchInterval || 1000;
  this.batches.push(config);
};

Batcher.prototype.eval = function(sha){
  if (!this.running) return;

  const date = new Date();
  async.each(this.batches, (batch, next) => {
    this.client.evalsha(
      sha, 4,
      `${this.prefix}${batch.sourceQueue}`, batch.sourceName, `${this.prefix}${batch.targetQueue}`, batch.targetName,
      batch.batchInterval, batch.batchSize, this.generateJobId(), date.getTime(), date.toISOString(),
      next
    )
  }, err => {
    if (err) throw err;
    setTimeout(() => this.eval(sha), 200);
  });
};

Batcher.prototype.stop = function(){
  this.running = false;
};

Batcher.prototype.generateJobId = function(){
  return new ObjectId().toString();
};

exports.makeBatcher = function(connectionOptions, options){
  return new Batcher(connectionOptions, options || {});
};