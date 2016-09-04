'use strict';
const sinon = require('sinon');
const should = require('should');

const JobContext = require('../../lib/job-context');

describe('job-context', function(){
  let context, job, worker;
  beforeEach(function(){
    job = {attempt: 2};
    worker = {
      retry: sinon.stub()
    };
    context = JobContext.create(job, worker);
  });
  describe('retryIn', function(){
    it('should pass job to worker.retry', function(){
      context.retryIn(100);
      worker.retry.callCount.should.equal(1);
      const args = worker.retry.getCall(0).args;
      args[0].should.equal(job);
      args[1].should.equal(100);
    });
    it('should throw if provided delay is not a number', function(){
      (function(){
        context.retryIn('invalid');
      }).should.throw('Delay must be a milliseconds integer value');
    })
  });
});