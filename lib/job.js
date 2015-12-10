'use strict';
var _ = require('underscore');
var ObjectID = require('mongodb').ObjectID;

function parse(job){
  try{
    job.params = JSON.parse(job.params);
  }catch(e){}
}

function Job(queue, name, params){
  if (queue && arguments.length === 1){
    _.extend(this, queue);
    if (this.enqueued) this.enqueued = new Date(this.enqueued);
    if (this.dequeued) this.dequeued = new Date(this.dequeued);
    if (this.failed) this.failed = new Date(this.failed);
    if (this.complete) this.complete = new Date(this.complete);
  }else{
    this.id = new ObjectID().toHexString();
    this.queue = queue;
    this.name = name;
    this.params = params;
    this.enqueued = new Date().toISOString();
  }
  parse(this);
}

Job.prototype.serialize = function(){
  return _.extend({}, _.omit(this, 'serialize'), {params: _.isObject(this.params) ? JSON.stringify(this.params) : this.params});
};

exports.create = function(queue, name, params){
  return new Job(queue, name, params);
};
