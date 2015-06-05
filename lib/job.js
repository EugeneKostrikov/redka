'use strict';
var _ = require('underscore');
var ObjectID = require('mongodb').ObjectID;

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
}

module.exports = Job;