'use strict';
const sinon = require('sinon');
const should = require('should');

const mongodb = require('mongodb');

const Job = require('../../lib/job');

describe('Job', function(){
  let clock;
  beforeEach(function(){
    sinon.stub(mongodb, 'ObjectID');
    clock = sinon.useFakeTimers();
  });
  afterEach(function(){
    clock.restore();
    mongodb.ObjectID.restore();
  });
  it('should initialize correct new job', function(){
    mongodb.ObjectID.returns('generated id');
    const job = Job.create('queue', 'name', 'params');
    job.id.should.equal('generated id');
    job.queue.should.equal('queue');
    job.name.should.equal('name');
    job.params.should.equal('params');
    job.enqueued.toString().should.equal(new Date().toString());
  });
  it('should correctly wrap existing job', function(){
    const source = {
      id: 'stored id',
      queue: 'queue',
      name: 'name',
      params: 'params',
      enqueued: new Date().toISOString(),
      dequeued: new Date().toISOString(),
      failed: new Date().toISOString(),
      complete: new Date().toISOString()
    };
    const job = Job.create(source);
    job.id.should.equal('stored id');
    job.queue.should.equal('queue');
    job.name.should.equal('name');
    job.params.should.equal('params');
    job.enqueued.toString().should.equal(new Date().toString());
    job.dequeued.toString().should.equal(new Date().toString());
    job.failed.toString().should.equal(new Date().toString());
    job.complete.toString().should.equal(new Date().toString());
  });
});