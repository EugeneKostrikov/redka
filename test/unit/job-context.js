'use strict';
const sinon = require('sinon');
const should = require('should');

const JobContext = require('../../lib/job-context');

describe('job-context', function(){
  let context, job, callback;
  beforeEach(function(){
    job = {
      attempt: 2,
      retry: sinon.stub()
    };
    callback = sinon.stub();
    context = JobContext.create(job, callback);
  });
  describe('retryIn', function(){
    it('should mark the job as retry', function(){
      context.retryIn(100);
      job.retry.callCount.should.equal(1);
      const args = job.retry.getCall(0).args;
      args[0].should.equal(100);
    });
    it('should call worker callback', function(){
      context.retryIn(100);
      callback.callCount.should.equal(1);
      should.not.exist(callback.getCall(0).args[0]);
    });
    it('should throw if provided delay is not a number', function(){
      (function(){
        context.retryIn('invalid');
      }).should.throw('Delay must be a milliseconds integer value');
    })
  });
});