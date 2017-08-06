"use strict";
const _ = require('underscore');
const debug = require('debug')('redka:job-events');
const RedisClient = require('./redis-client');
const Job = require('./job');

function JobEventEmitter(connectionOptions){
  this.client = RedisClient.initialize(connectionOptions);
}

JobEventEmitter.prototype.event = function(type, job){
  return {
    type: type,
    timestamp: new Date().toISOString(),
    job: job.serialize()
  };
};

JobEventEmitter.prototype.emitEvent = function(event){
  debug(`Emitting ${event.type} event for job ${event.job.id}`);
  this.client.publish(`redka-job-events:${event.type}`, JSON.stringify(event));
};

[
  ['jobEnqueued', 'ENQUEUED'],
  ['jobDequeued', 'DEQUEUED'],
  ['jobRetry', 'RETRY'],
  ['jobComplete', 'COMPLETE'],
  ['jobFailed', 'FAILED']
].forEach(pair => {
  JobEventEmitter.prototype[pair[0]] = function(job){
    this.emitEvent(this.event(pair[1], job));
  };
});

exports.createEmitter = function(connectionOptions){
  return new JobEventEmitter(connectionOptions);
};

function JobEventsReceiver(connectionOptions){
  this._channels = [
    'redka-job-events:ENQUEUED',
    'redka-job-events:DEQUEUED',
    'redka-job-events:RETRY',
    'redka-job-events:COMPLETE',
    'redka-job-events:FAILED'
  ];

  this.subscriber = RedisClient.initialize(connectionOptions);
  this._callbacks = {};
  this._channels.forEach(channel => {
    this.subscriber.subscribe(channel);
    this._callbacks[channel] = {};
  });

  this.subscriber.on('message', (channel, message) => {
    _.values(this._callbacks[channel]).forEach(callback => {
      const msg = JSON.parse(message);
      msg.job = Job.create(msg.job);
      debug(`Received ${msg.type} event for job ${msg.job.id}`);
      callback(msg);
    });
  });
}

JobEventsReceiver.prototype.subscribeTo = function(eventType, callback){
  const channel = `redka-job-events:${eventType}`;
  const id = _.uniqueId();
  this._callbacks[channel][id] = callback;
  return () => {
    delete this._callbacks[channel][id];
  };
};

[
  ['onEnqueued', 'ENQUEUED'],
  ['onDequeued', 'DEQUEUED'],
  ['onRetry', 'RETRY'],
  ['onComplete', 'COMPLETE'],
  ['onFailed', 'FAILED']
].forEach(function(pair){
  JobEventsReceiver.prototype[pair[0]] = function(callback){
    return this.subscribeTo(pair[1], callback);
  };
});

exports.createReceiver = function(connectionOptions){
  return new JobEventsReceiver(connectionOptions);
};
