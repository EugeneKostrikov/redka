'use strict';
const _ = require('underscore');
const mongodb = require('mongodb');
const assert = require('assert');

function parse(job){
  job.params = tryParse(job.params);
}

function tryParse(jsonLikeString){
  try {
    return JSON.parse(jsonLikeString);
  }catch(e){
    return jsonLikeString;
  }
}

function Job(queue, name, params, options){
  if (queue && !name && !params){
    _.extend(this, queue);
    if (this.enqueued) this.enqueued = new Date(this.enqueued);
    if (this.dequeued) this.dequeued = new Date(this.dequeued);
    if (this.failed) this.failed = new Date(this.failed);
    if (this.complete) this.complete = new Date(this.complete);
    if (this.result) this.result = tryParse(this.result);
    if (this.error) this.error = tryParse(this.error);
    if (this.delay) this.delay = parseInt(this.delay);
    //Available on parsed job only
    if (this.attempt) this.attempt = parseInt(this.attempt);
    if (this.notes) this.notes = tryParse(this.notes);
  }else{
    this.id = mongodb.ObjectID().toString();
    this.queue = queue;
    this.name = name;
    this.params = params;
    this.enqueued = new Date();
    this.delay = 0;
    this.attempt = 1;
    this.notes = {};
  }
  if (options && options.delay){
    if (_.isDate(options.delay)){
      this.delay = options.delay.getTime();
    }else if (_.isNumber(parseInt(options.delay))){
      let d = parseInt(options.delay);
      assert(d >= 0, 'Delay must be greater than zero');
      this.delay = Date.now() + d;
    }
  }
  parse(this);
}

Job.prototype.note = function(key, val){
  this.notes[key] = val;
};

Job.prototype.look = function(key){
  return this.notes[key];
};

Job.prototype.retry = function(delay){
  this.delay = Date.now() + delay;
  this.complete = true;
  this.status = 'retry';
};

Job.prototype.serialize = function(){
  return _.extend({}, _.omit(this, 'serialize', 'retry', 'look', 'note'),
   this.params ? {params: _.isObject(this.params) ? JSON.stringify(this.params) : this.params} : {},
   this.result ? {result: _.isObject(this.result) ? JSON.stringify(this.result) : this.result} : {},
   this.error ? {error: _.isObject(this.error) ? JSON.stringify(this.error) : this.error} : {},
   this.notes ? {notes: JSON.stringify(this.notes)} : {}
  );
};

exports.create = function(queue, name, params, options){
  return new Job(queue, name, params, options);
};