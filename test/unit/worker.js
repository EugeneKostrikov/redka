'use strict';
var sinon = require('sinon');
var should = require('should');
var helpers = require('./helpers');
var Job = require('../../lib/job');

//Stubbed
var redisClient = require('../../lib/redis-client');

//SUT
var workerModule = require('../../lib/worker');

module.exports = function(){
  describe('worker', function(){
    var redis, worker, cb;
    beforeEach(function(){
      redis = helpers.mockRedis();
      sinon.stub(redisClient, 'initialize').returns(redis);
      worker = workerModule.create('test', {}, {timeout: 10});
      sinon.stub(worker, 'dequeue'); //TO prevent infinite loop;
      cb = sinon.stub().yieldsAsync(null);
      worker.register({testing: cb});
    });
    afterEach(function(){
      redisClient.initialize.restore();
    });
    describe('plumbing', function(){
      beforeEach(function(){
        worker.dequeue.restore();
        sinon.stub(worker, 'timeout');
      });
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
    describe('register', function(){
      beforeEach(function(){
        sinon.stub(worker, 'poll');
      });
      afterEach(function(){
        worker.poll.restore();
      });
      it('should extend registered callbacks', function(){
        worker.register({nextcb: function(){}});
        worker.callbacks.should.have.keys(['testing','nextcb']);
      });
      it('should start polling for jobs immediately after callbacks were registered', function(){
        worker.status = 'INIT';
        worker.register({new: function(){}});
        worker.poll.callCount.should.equal(1);
      });
      it('should not start polling for new jobs when callbacks are registered if worker state is not INIT', function(){
        worker.status = 'RUNNING';
        worker.register({new: function(){}});
        worker.poll.callCount.should.equal(0);
      });
    });
    describe('poll', function(){
      beforeEach(function(){
        worker.dequeue.restore();
        sinon.stub(worker, 'dequeue');
        sinon.stub(worker, 'handleError');
        sinon.stub(worker, 'fail');
        sinon.stub(worker, 'complete');
        sinon.stub(worker, 'work');
      });
      afterEach(function(){
        worker.dequeue.restore();
        worker.handleError.restore();
        worker.fail.restore();
        worker.complete.restore();
        worker.work.restore();
      });
      it('should dequeue a job', function(){
        let job = {};
        worker.dequeue.yields(null, job);
        worker.poll();
        worker.dequeue.callCount.should.equal(1);
      });
      it('should pass possible error to handleError', function(){
        let err = {};
        worker.dequeue.yields(err);
        worker.poll();
        worker.handleError.callCount.should.equal(1);
        worker.handleError.getCall(0).args[0].should.equal(err);
      });
      it('should pass the job to work', function(){
        let job = {};
        worker.dequeue.yields(null, job);
        worker.poll();
        worker.work.callCount.should.equal(1);
        worker.work.getCall(0).args[0].should.equal(job);
      });
      it('should fail the job if worker called back with error', function(){
        let job = {};
        let err = {};
        worker.dequeue.yields(null, job);
        worker.work.yields(err);
        worker.poll();
        worker.fail.callCount.should.equal(1);
        worker.fail.getCall(0).args[0].should.equal(job);
        worker.fail.getCall(0).args[1].should.equal(err);
      });
      it('should complete the job if worker called back with no error', function(){
        let job = {};
        let result = {};
        worker.dequeue.yields(null, job);
        worker.work.yields(null, result);
        worker.poll();
        worker.complete.callCount.should.equal(1);
        worker.complete.getCall(0).args[0].should.equal(job);
        worker.complete.getCall(0).args[1].should.equal(result);
      });
      it('should correctly set statuses', function(){
        worker.status = 'INIT';
        worker.poll();
        worker.status.should.equal('POLLING');
        worker.dequeue.yield(null);
        worker.status.should.equal('WORKING');
        worker.work.yield(null);
        worker.status.should.equal('FINISHING');
      });
      it('should not pick up new job if the status is STOPPING', function(){
        worker.status = 'STOPPING';
        worker.poll();
        worker.status.should.equal('STOPPED');
        worker.dequeue.callCount.should.equal(0);
      });
      it('shouls switch status to STOPPED and return once dequeue calls back with no job', function(){
        worker.poll();
        worker.status = 'STOPPING';
        worker.dequeue.yield();
        worker.status.should.equal('STOPPED');
        worker.work.callCount.should.equal(0);
      });
    });
    describe('enqueue', function(){
      beforeEach(function(){
        sinon.spy(worker, 'handleError');
      });
      afterEach(function(){
        worker.handleError.restore();
      });
      it('should initialise the job', function(done){
        worker.enqueue('name', 'params', function(){
          let job = worker.client.hmset.getCall(0).args[1];
          should.exist(job.id);
          job.params.should.equal('params');
          job.name.should.equal('name');
          done();
        });
      });
      it('should set the job params to job.id key', function(done){
        worker.enqueue('name', 'params', function(){
          let job = worker.client.hmset.getCall(0).args[1];
          worker.client.hmset.callCount.should.equal(1);
          worker.client.hmset.getCall(0).args[0].should.equal(job.id);
          done();
        });
      });
      it('should pass possible hmset error to handleError', function(done){
        let err = {};
        worker.client.hmset.yieldsAsync(err);
        worker.enqueue('name', 'params', function(err){
          should.exist(err);
          worker.handleError.callCount.should.equal(1);
          worker.handleError.getCall(0).args[0].should.equal(err);
          done();
        });
      });
      it('should push the job id to pending list', function(done){
        worker.enqueue('name', 'params', function(){
          worker.client.lpush.callCount.should.equal(1);
          worker.client.lpush.getCall(0).args[0].should.equal('test_pending');
          let jobId = worker.client.hmset.getCall(0).args[1].id;
          worker.client.lpush.getCall(0).args[1].should.equal(jobId);
          done();
        });
      });
      it('should pass possible push error to handleError', function(done){
        let err = {};
        worker.client.lpush.yieldsAsync(err);
        worker.enqueue('name', 'params', function(err){
          should.exist(err);
          worker.handleError.callCount.should.equal(1);
          worker.handleError.getCall(0).args[0].should.equal(err);
          done();
        });
      });
    });
    describe('dequeue', function(){
      beforeEach(function(){
        worker.dequeue.restore();
        sinon.spy(worker, 'dequeue');
        sinon.stub(worker, 'timeout');
        sinon.stub(Job, 'create').returns({});
      });
      afterEach(function(){
        worker.dequeue.restore();
        worker.timeout.restore();
        Job.create.restore();
      });
      it('should set a blocking pop and push from pending list to progress list', function(done){
        worker.pollingClient.brpoplpush.onFirstCall().yieldsAsync('stop');
        worker.dequeue(function(err){
          err.should.equal('stop');
          worker.pollingClient.brpoplpush.callCount.should.equal(1);
          worker.pollingClient.brpoplpush.getCall(0).args[0].should.equal('test_pending');
          worker.pollingClient.brpoplpush.getCall(0).args[1].should.equal('test_progress');
          done();
        });
      });
      it('should release the block in 1 second', function(done){
        worker.pollingClient.brpoplpush.onFirstCall().yieldsAsync('stop');
        worker.dequeue(function(err){
          err.should.equal('stop');
          worker.pollingClient.brpoplpush.getCall(0).args[2].should.equal(1);
          done();
        });
      });
      it('should iterate if pop didnt return any result', function(done){
        worker.pollingClient.brpoplpush.onSecondCall().yieldsAsync('stop');
        worker.dequeue(function(err){
          err.should.equal('stop');
          worker.pollingClient.brpoplpush.callCount.should.equal(2);
          done();
        });
      });
      it('should set dequeue date on the job', function(done){
        worker.pollingClient.brpoplpush.yieldsAsync(null, 'id');
        worker.client.multi().exec.yieldsAsync('stop');
        worker.dequeue(function(err){
          err.should.equal('stop');
          let m = worker.client.multi();
          m.hset.callCount.should.equal(1);
          m.hset.getCall(0).args[0].should.equal('id');
          m.hset.getCall(0).args[1].should.equal('dequeued');
          done();
        });
      });
      it('should fetch full job params', function(done){
        worker.pollingClient.brpoplpush.yieldsAsync(null, 'id');
        worker.client.multi().exec.yieldsAsync('stop');
        worker.dequeue(function(err){
          err.should.equal('stop');
          let m = worker.client.multi();
          m.hgetall.callCount.should.equal(1);
          m.hgetall.getCall(0).args[0].should.equal('id');
          done();
        });
      });
      it('should callback if fetch fails', function(done){
        worker.pollingClient.brpoplpush.yieldsAsync(null, 'id');
        worker.client.multi().exec.yieldsAsync('err');
        worker.dequeue(function(err){
          err.should.equal('err');
          done();
        });
      });
      it('should initialize the job', function(done){
        worker.pollingClient.brpoplpush.yieldsAsync(null, 'id');
        let jobData = {};
        worker.client.multi().exec.yieldsAsync(null, ['date', jobData]);
        worker.dequeue(function(err){
          should.not.exist(err);
          Job.create.callCount.should.equal(1);
          Job.create.getCall(0).args[0].should.equal(jobData);
          done();
        });
      });
      it('should set timeout on the job', function(done){
        worker.pollingClient.brpoplpush.yieldsAsync(null, 'id');
        worker.client.multi().exec.yieldsAsync(null, ['date', {}]);
        worker.dequeue(function(err, job){
          should.not.exist(err);
          worker.timeout.callCount.should.equal(1);
          worker.timeout.getCall(0).args[0].should.equal(job);
          done();
        });
      });
      it('should not try dequeueing job if status is STOPPING', function(done){
        worker.status = 'STOPPING';
        worker.dequeue(function(err, job){
          should.not.exist(err);
          should.not.exist(job);
          done();
        });
      });
    });
    describe('timeout', function(){
      let clock;
      beforeEach(function(){
        clock = sinon.useFakeTimers();
        sinon.stub(worker, 'fail');
      });
      afterEach(function(){
        clock.restore();
        worker.fail.restore();
      });
      it('should not set timeout if it is not configured for worker', function(){
        worker.to = null;
        worker.timeout({id: 'id'});
        should.not.exist(worker._timeouts.id);
      });
      it('should fail the job once timeout fires', function(){
        let job = {id: 'id'};
        worker.timeout(job);
        clock.tick(11);
        worker.fail.callCount.should.equal(1);
        worker.fail.getCall(0).args[0].should.equal(job);
      });
      it('should keep track of timeouts in _timeouts', function(){
        worker.timeout({id: 'one'});
        worker.timeout({id: 'two'});
        should.exist(worker._timeouts.one);
        should.exist(worker._timeouts.two);
      });
    });
    describe('clearTimeout', function(){
      beforeEach(function(){
        sinon.spy(global, 'clearTimeout');
      });
      afterEach(function(){
        global.clearTimeout.restore();
      });
      it('should do nothing if timeout is not configured on worker', function(){
        worker._timeouts = null;
        worker.clearTimeout({id: 'id'});
        global.clearTimeout.called.should.not.be.ok;
      });
      it('should exit if theres no timeout for provided job', function(){
        worker._timeouts = {};
        worker.clearTimeout({id: 'id'});
        global.clearTimeout.called.should.not.be.ok;
      });
      it('should clear the timeout', function(){
        worker._timeouts = {id: setTimeout(function(){
          throw new Error('Failed!');
        }, 0)};
        worker.clearTimeout({id: 'id'});
        global.clearTimeout.called.should.be.ok;
      });
      it('should drop the timeout from _timeouts', function(){
        worker._timeouts = {id: setTimeout(function(){
          throw new Error('Failed!');
        }, 0)};
        worker.clearTimeout({id: 'id'});
        should.not.exist(worker._timeouts.id);
      });
    });
    describe('fail', function(){
      let job, error;
      beforeEach(function(){
        sinon.spy(worker, 'handleError');
        sinon.stub(worker, 'clearTimeout');
        sinon.stub(worker, 'poll');
        job = {id: 'jobid'};
        error = {stack: 'stack'};
      });
      afterEach(function(){
        worker.handleError.restore();
        worker.clearTimeout.restore();
        worker.poll.restore();
      });
      it('should save error and stack on the job', function(done){
        worker.client.lrange.yieldsAsync('stop');

        worker.fail(job, error, function(err){
          err.should.equal('stop');
          job.error.should.equal(error);
          job.error.stack.should.equal('stack');
          done();
        });
      });
      it('should exit if the job cannot be found in progress list', function(done){
        worker.client.lrange.yieldsAsync(null, []);
        worker.fail(job, error, function(err){
          should.not.exist(err);
          worker.client.multi().hmset.called.should.not.be.ok;
          done();
        });
      });
      it('should immediately callback if lrange fails', function(done){
        worker.client.lrange.yieldsAsync('stop');
        worker.fail(job, error, function(err){
          err.should.equal('stop');
          worker.client.multi().hmset.called.should.not.be.ok;
          done();
        });
      });
      it('should persist status to failed', function(done){
        worker.client.lrange.yieldsAsync(null, ['jobid']);
        worker.fail(job, error, function(err){
          should.not.exist(err);
          worker.client.multi().hmset.callCount.should.equal(1);
          worker.client.multi().hmset.getCall(0).args[0].should.equal('jobid');
          worker.client.multi().hmset.getCall(0).args[1].status.should.equal('failed');
          done();
        });
      });
      it('should push the job id to failed list', function(done){
        worker.client.lrange.yieldsAsync(null, ['jobid']);
        worker.fail(job, error, function(err){
          should.not.exist(err);
          worker.client.multi().lpush.callCount.should.equal(1);
          worker.client.multi().lpush.getCall(0).args[0].should.equal('test_failed');
          worker.client.multi().lpush.getCall(0).args[1].should.equal('jobid');
          done();
        });
      });
      it('should remove the job id from progress list', function(done){
        worker.client.lrange.yieldsAsync(null, ['jobid']);
        worker.fail(job, error, function(err){
          should.not.exist(err);
          worker.client.multi().lrem.callCount.should.equal(1);
          worker.client.multi().lrem.getCall(0).args[0].should.equal('test_progress');
          worker.client.multi().lrem.getCall(0).args[2].should.equal('jobid');
          done();
        });
      });
      it('should callback immediately if set/push/rem fail', function(done){
        worker.client.lrange.yieldsAsync(null, ['jobid']);
        worker.client.multi().exec.yieldsAsync('stop');
        worker.fail(job, error, function(err){
          err.should.equal('stop');
          worker.handleError.callCount.should.equal(1);
          worker.handleError.getCall(0).args[0].should.equal('stop');
          done();
        });
      });
      it('should clear timeout on the job', function(done){
        worker.client.lrange.yieldsAsync(null, ['jobid']);
        worker.client.multi().exec.yieldsAsync(null);
        worker.fail(job, error, function(err){
          should.not.exist(err);
          worker.clearTimeout.called.should.be.ok;
          worker.clearTimeout.getCall(0).args[0].should.equal(job);
          done();
        });
      });
      it('should poll for the next job', function(done){
        worker.client.lrange.yieldsAsync(null, ['jobid']);
        worker.client.multi().exec.yieldsAsync(null);
        worker.fail(job, error, function(err){
          should.not.exist(err);
          worker.poll.called.should.be.ok;
          done();
        });
      });
    });
    describe('complete', function(){
      let job, result;
      beforeEach(function(){
        sinon.spy(worker, 'handleError');
        sinon.stub(worker, 'clearTimeout');
        sinon.stub(worker, 'poll');
        job = {id: 'jobid'};
        result = {};
      });
      afterEach(function(){
        worker.handleError.restore();
        worker.clearTimeout.restore();
        worker.poll.restore();
      });
      it('should set result to the job', function(done){
        worker.client.lrange.yieldsAsync('stop');
        worker.complete(job, result, function(err){
          err.should.equal('stop');
          job.result.should.equal(result);
          done();
        });
      });
      it('should exit if the job is not on progress list', function(done){
        worker.client.lrange.yieldsAsync(null, []);
        worker.complete(job, result, function(err){
          should.not.exist(err);
          worker.client.multi().hmset.called.should.not.be.ok;
          done();
        });
      });
      it('should callback immediately if range fails', function(done){
        worker.client.lrange.yieldsAsync('stop');
        worker.complete(job, result, function(err){
          err.should.equal('stop');
          worker.client.multi().hmset.called.should.not.be.ok;
          done();
        });
      });
      it('should set job status to complete', function(done){
        worker.client.lrange.yieldsAsync(null, ['jobid']);
        worker.complete(job, result, function(err){
          should.not.exist(err);
          worker.client.multi().hmset.callCount.should.equal(1);
          worker.client.multi().hmset.getCall(0).args[0].should.equal('jobid');
          worker.client.multi().hmset.getCall(0).args[1].status.should.equal('complete');
          done();
        });
      });
      it('should push job id to complete list', function(done){
        worker.client.lrange.yieldsAsync(null, ['jobid']);
        worker.complete(job, result, function(err){
          should.not.exist(err);
          worker.client.multi().lpush.callCount.should.equal(1);
          worker.client.multi().lpush.getCall(0).args[0].should.equal('test_complete');
          worker.client.multi().lpush.getCall(0).args[1].should.equal('jobid');
          done();
        });
      });
      it('should remove job id from progress list', function(done){
        worker.client.lrange.yieldsAsync(null, ['jobid']);
        worker.complete(job, result, function(err){
          should.not.exist(err);
          worker.client.multi().lrem.callCount.should.equal(1);
          worker.client.multi().lrem.getCall(0).args[0].should.equal('test_progress');
          worker.client.multi().lrem.getCall(0).args[2].should.equal('jobid');
          done();
        });
      });
      it('should handle error if set/push/rem fail', function(done){
        worker.client.lrange.yieldsAsync(null, ['jobid']);
        worker.client.multi().exec.yieldsAsync('stop');
        worker.complete(job, result, function(err){
          err.should.equal('stop');
          worker.handleError.callCount.should.equal(1);
          worker.handleError.getCall(0).args[0].should.equal('stop');
          done();
        });
      });
      it('should clear job timeout', function(done){
        worker.client.lrange.yieldsAsync(null, ['jobid']);
        worker.client.multi().exec.yieldsAsync(null);
        worker.complete(job, result, function(err){
          should.not.exist(err);
          worker.clearTimeout.called.should.be.ok;
          worker.clearTimeout.getCall(0).args[0].should.equal(job);
          done();
        });
      });
      it('should poll for the next job', function(done){
        worker.client.lrange.yieldsAsync(null, ['jobid']);
        worker.client.multi().exec.yieldsAsync(null);
        worker.complete(job, result, function(err){
          should.not.exist(err);
          worker.poll.called.should.be.ok;
          done();
        });
      });
    });
    describe('handleError', function(){
      it('should pass the error to callback if it is provided', function(done){
        worker.handleError('err', function(e){
          e.should.equal('err');
          done();
        });
      });
      it('should do nothing if callback is not provided', function(){
        worker.handleError('err');
      });
    });
    describe('throwToBacklog', function(){
      let job;
      beforeEach(function(){
        job = {id: 'jobid'};
        sinon.stub(worker, 'clearTimeout');
        sinon.stub(worker, 'poll');
        sinon.spy(worker, 'handleError');
      });
      afterEach(function(){
        worker.clearTimeout.restore();
        worker.poll.restore();
        worker.handleError.restore();
      })
      it('should do nothing if the job is not in progress list', function(done){
        worker.client.lrange.yieldsAsync(null, []);
        worker.throwToBacklog(job, function(err){
          should.not.exist(err);
          worker.client.multi().lpush.callCount.should.equal(0);
          done();
        });
      });
      it('should immediately handle error if range fails', function(done){
        worker.client.lrange.yieldsAsync('stop');
        worker.throwToBacklog(job, function(err){
          err.should.equal('stop');
          worker.client.multi().lpush.callCount.should.equal(0);
          done();
        });
      });
      it('should push job id to backlog list', function(done){
        worker.client.lrange.yieldsAsync(null, ['jobid']);
        worker.client.multi().exec.yieldsAsync(null);
        worker.throwToBacklog(job, function(err){
          should.not.exist(err);
          worker.client.multi().lpush.callCount.should.equal(1);
          worker.client.multi().lpush.getCall(0).args[0].should.equal('test_backlog');
          worker.client.multi().lpush.getCall(0).args[1].should.equal('jobid');
          done();
        });
      });
      it('should remove job id from progress list', function(done){
        worker.client.lrange.yieldsAsync(null, ['jobid']);
        worker.client.multi().exec.yieldsAsync(null);
        worker.throwToBacklog(job, function(err){
          should.not.exist(err);
          worker.client.multi().lrem.callCount.should.equal(1);
          worker.client.multi().lrem.getCall(0).args[0].should.equal('test_progress');
          worker.client.multi().lrem.getCall(0).args[2].should.equal('jobid');
          done();
        });
      });
      it('should clear job timeout', function(done){
        worker.client.lrange.yieldsAsync(null, ['jobid']);
        worker.client.multi().exec.yieldsAsync(null);
        worker.throwToBacklog(job, function(err){
          should.not.exist(err);
          worker.clearTimeout.callCount.should.equal(1);
          worker.clearTimeout.getCall(0).args[0].should.equal(job);
          done();
        });
      });
      it('should poll for new job', function(done){
        worker.client.lrange.yieldsAsync(null, ['jobid']);
        worker.client.multi().exec.yieldsAsync(null);
        worker.throwToBacklog(job, function(err){
          should.not.exist(err);
          worker.poll.callCount.should.equal(1);
          done();
        });
      });
      it('should handle possible push/rem error', function(done){
        worker.client.lrange.yieldsAsync(null, ['jobid']);
        worker.client.multi().exec.yieldsAsync('err');
        worker.throwToBacklog(job, function(err){
          err.should.equal('err');
          worker.handleError.callCount.should.equal(1);
          done();
        });
      });
    });
    describe('work', function(){
      let job;
      beforeEach(function(){
        job = {name: 'testing', params: {}};
        sinon.stub(worker, 'throwToBacklog').yieldsAsync(null);
      });
      afterEach(function(){
        worker.throwToBacklog.restore();
      });
      it('should throw the job to backlog if no callback is registered for job name', function(done){
        let job = {name: 'unknown'};
        worker.work(job, function(err){
          should.not.exist(err);
          worker.throwToBacklog.callCount.should.equal(1);
          worker.throwToBacklog.getCall(0).args[0].should.equal(job);
          done();
        });
      });
      it('should call the worker providing job params', function(done){
        worker.work(job, function(err){
          should.not.exist(err);
          cb.callCount.should.equal(1);
          cb.getCall(0).args[0].should.eql({});
          done();
        });
      });
      it('should mark the job as complete once the worker is done', function(done){
        worker.work(job, function(err){
          should.not.exist(err);
          job.complete.should.be.ok;
          done();
        });
      });
      it('should error if worker callback is called twice', function(){
        cb.yields(null);
        let callback = sinon.stub();
        worker.work(job, callback);
        callback.callCount.should.equal(1);
        should.not.exist(callback.getCall(0).args[0]);
        cb.yield(null);
        callback.callCount.should.equal(2);
        callback.getCall(1).args[0].message.should.match(/callback is called twice/);
      });
    });
    describe('stop', function(){
      let clock;
      beforeEach(function(){
        clock = sinon.useFakeTimers();
      });
      afterEach(function(){
        clock.restore();
      });
      it('should set worker status to STOPPING', function(){
        worker.status.should.equal('POLLING');
        worker.stop(function(){});
        worker.status.should.equal('STOPPING');
      });
      it('should callback once status is changed to STOPPED', function(done){
        worker.status.should.equal('POLLING');
        worker.stop(done);
        worker.status = 'STOPPED';
        clock.tick(101);
      });
    });
  });
};