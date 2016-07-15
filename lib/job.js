'use strict';
var _ = require('underscore');
var mongodb = require('mongodb');

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

function Job(queue, name, params){
  if (queue && !name && !params){
    _.extend(this, queue);
    if (this.enqueued) this.enqueued = new Date(this.enqueued);
    if (this.dequeued) this.dequeued = new Date(this.dequeued);
    if (this.failed) this.failed = new Date(this.failed);
    if (this.complete) this.complete = new Date(this.complete);
    if (this.result) this.result = tryParse(this.result);
    if (this.error) this.error = tryParse(this.error);
  }else{
    this.id = mongodb.ObjectID().toString();
    this.queue = queue;
    this.name = name;
    this.params = params;
    this.enqueued = new Date();
  }
  parse(this);
}

Job.prototype.serialize = function(){
  return _.extend({}, _.omit(this, 'serialize'), {params: _.isObject(this.params) ? JSON.stringify(this.params) : this.params});
};

exports.create = function(queue, name, params){
  return new Job(queue, name, params);
};
