'use strict';
var sinon = require('sinon');
var should = require('should');
var helpers = require('./helpers');

//Stubbed
var redisClient = require('../../lib/redis-client');

//SUT
var Worker = require('../../lib/worker');

module.exports = function(){
  describe('worker', function(){
    var redis, worker, cb;
    beforeEach(function(){
      redis = helpers.mockRedis();
      sinon.stub(redisClient, 'initialize').returns(redis);
      worker = new Worker('test', {}, {timeout: 10});
      sinon.stub(worker, 'timeout');
      cb = sinon.stub().yieldsAsync(null);
      worker.register({testing: cb});
    });
    afterEach(function(){
      redisClient.initialize.restore();
    });
    it('should be bound to single queue');
    it('should be able to process many job names');
    it('should pick next job once previous is complete');
    it('should wait for new jobs when nothing can be pulled out of the queue');
    describe('plumbing', function(){
      it('should be polling for jobs until it hits one', function(done){
        redis.brpoplpush.onFirstCall().yieldsAsync(null, null);
        redis.brpoplpush.onSecondCall().yieldsAsync(null, 'job id');
        redis.brpoplpush.onThirdCall().yieldsAsync(null, 'job id');
        redis.multi().exec.yieldsAsync(null, []);
        worker.dequeue(function(err){
          should.not.exist(err);
          redis.brpoplpush.callCount.should.equal(2);
          done();
        });
      });
      /*    it('should start processing job once worker hits one', function(done){
       sinon.stub(worker, 'work', function(){
       worker.work.calledOnce.should.be.ok;
       worker.work.restore();
       done();
       });
       redis.brpoplpush.onFirstCall().yieldsAsync(null, null);
       redis.brpoplpush.onSecondCall().yieldsAsync(null, 'job');
       redis.multi().exec.yieldsAsync(null, []);
       worker.poll()
       });*/
      it('should fail errored jobs', function(done){
        sinon.stub(worker, 'work').yieldsAsync('error');
        sinon.stub(worker, 'dequeue').yieldsAsync(null, 'job');
        redis.lrange.yieldsAsync(null, ['1234']);
        sinon.stub(worker, 'fail', function(){
          worker.fail.calledOnce.should.be.ok;
          done();
        });

        worker.poll();
      });
      it('should complete successful jobs', function(done){
        sinon.stub(worker, 'work').yieldsAsync(null, 'ok');
        sinon.stub(worker, 'dequeue').yieldsAsync(null, 'job');
        redis.lrange.yieldsAsync(null, ['1234']);
        sinon.stub(worker, 'complete', function(){
          worker.complete.calledOnce.should.be.ok;
          done();
        });
        worker.poll();
      });
      it('should timeout stuck jobs', function(done){
        worker.timeout.restore();
        sinon.stub(worker, 'poll');
        redis.brpoplpush.yieldsAsync(null, 'job');
        redis.multi().exec.yieldsAsync(null, []);
        redis.lrange.yieldsAsync(null, ['1234']);
        sinon.stub(worker, 'fail', function(job, error){
          error.message.should.equal('Worker timed out');
          done();
        });
        worker.dequeue(function(){});
      });
      it('should clear timeout when the job is explicitly complete', function(done){
        worker.timeout.restore();
        sinon.stub(worker, 'poll');
        redis.brpoplpush.yieldsAsync(null, 'job');
        redis.multi().exec.yieldsAsync(null, []);
        sinon.stub(worker, 'fail');
        redis.lrange.yieldsAsync(null, ['1234']);
        worker.dequeue(function(){
          worker.complete({id: '1234'}, '', function(){
            setTimeout(function(){
              worker.fail.called.should.not.be.ok;
              done();
            }, 20);
          });
        });
      });
      it('should clear timeout when the job is explicitly failed', function(done){
        worker.timeout.restore();
        sinon.stub(worker, 'poll');
        redis.brpoplpush.yieldsAsync(null, 'job');
        redis.multi().exec.yieldsAsync(null, []);
        sinon.spy(worker, 'fail');
        redis.lrange.yieldsAsync(null, ['1234']);
        worker.dequeue(function(){
          worker.fail({id: '1234'}, '', function(){
            setTimeout(function(){
              worker.fail.calledOnce.should.be.ok;
              done();
            }, 20);
          });
        });
      });
      it('should not fail single job twice', function(done){
        var job = {id: '1234'};
        sinon.stub(worker, 'poll');
        redis.multi().exec.yieldsAsync(null, []);
        redis.lrange.onFirstCall().yieldsAsync(null, ['1234']);
        redis.lrange.onSecondCall().yieldsAsync(null, []);
        worker.fail(job, '', function(){
          worker.fail(job, '', function(){
            setImmediate(function(){
              worker.poll.calledOnce.should.be.ok;
              done();
            });
          });
        });
      });
      it('should not complete single job twice', function(done){
        var job = {id: '1234'};
        sinon.stub(worker, 'poll');
        redis.multi().exec.yieldsAsync(null, []);
        redis.lrange.onFirstCall().yieldsAsync(null, ['1234']);
        redis.lrange.onSecondCall().yieldsAsync(null, []);
        worker.complete(job, '', function(){
          worker.complete(job, '', function(){
            setImmediate(function(){
              worker.poll.calledOnce.should.be.ok;
              done();
            });
          });
        });
      });
      it('should not fail and complete a job at the same time', function(done){
        var job = {id: '1234'};
        sinon.stub(worker, 'poll');
        redis.multi().exec.yieldsAsync(null, []);
        redis.lrange.onFirstCall().yieldsAsync(null, ['1234']);
        redis.lrange.onSecondCall().yieldsAsync(null, []);
        worker.fail(job, '', function(){
          worker.complete(job, '', function(){
            setImmediate(function(){
              worker.poll.calledOnce.should.be.ok;
              done();
            });
          });
        });
      });
    });
  });
};