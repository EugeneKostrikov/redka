'use strict';
const sinon = require('sinon');
const should = require('should');
const helpers = require('./helpers');
const Job = require('../../lib/job');

//Stubbed
const redisClient = require('../../lib/redis-client');

//SUT
const workerModule = require('../../lib/worker');

module.exports = function(){
  describe('worker', function(){
    let redis, worker, cb;
    beforeEach(function(){
      redis = helpers.mockRedis();
      sinon.stub(redisClient, 'initialize').returns(redis);
      worker = workerModule.create('test', {}, {timeout: 10}, 'prefix');
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
        sinon.stub(worker, 'dequeue').yieldsAsync(null, {status: 'failed'});
        redis.lrange.yieldsAsync(null, ['1234']);
        sinon.stub(worker, 'fail', function(){
          worker.fail.calledOnce.should.be.ok;
          done();
        });

        worker.poll();
      });
      it('should complete successful jobs', function(done){
        sinon.stub(worker, 'work').yieldsAsync(null, 'ok');
        sinon.stub(worker, 'dequeue').yieldsAsync(null, {status: 'complete'});
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
        const job = {id: '1234'};
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
        const job = {id: '1234'};
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
        const job = {id: '1234'};
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
        sinon.stub(worker, 'retry');
      });
      afterEach(function(){
        worker.dequeue.restore();
        worker.handleError.restore();
        worker.fail.restore();
        worker.complete.restore();
        worker.work.restore();
        worker.retry.restore();
      });
      it('should dequeue a job', function(){
        const job = {};
        worker.dequeue.yields(null, job);
        worker.poll();
        worker.dequeue.callCount.should.equal(1);
      });
      it('should pass possible error to handleError', function(){
        const err = {};
        worker.dequeue.yields(err);
        worker.poll();
        worker.handleError.callCount.should.equal(1);
        worker.handleError.getCall(0).args[0].should.equal(err);
      });
      it('should pass the job to work', function(){
        const job = {};
        worker.dequeue.yields(null, job);
        worker.poll();
        worker.work.callCount.should.equal(1);
        worker.work.getCall(0).args[0].should.equal(job);
      });
      it('should fail the job if worker called back with error', function(){
        const job = {status: 'failed'};
        const err = {};
        worker.dequeue.yields(null, job);
        worker.work.yields(err);
        worker.poll();
        worker.fail.callCount.should.equal(1);
        worker.fail.getCall(0).args[0].should.equal(job);
        worker.fail.getCall(0).args[1].should.equal(err);
      });
      it('should complete the job if worker called back with no error', function(){
        const job = {status: 'complete'};
        const result = {};
        worker.dequeue.yields(null, job);
        worker.work.yields(null, result);
        worker.poll();
        worker.complete.callCount.should.equal(1);
        worker.complete.getCall(0).args[0].should.equal(job);
        worker.complete.getCall(0).args[1].should.equal(result);
      });
      it('should retry the job if worker called back with retry status', function(){
        const job = {status: 'retry'};
        const result = {};
        worker.dequeue.yields(null, job);
        worker.work.yields(null, result);
        worker.poll();
        worker.retry.callCount.should.equal(1);
        worker.retry.getCall(0).args[0].should.equal(job);
      });
      it('should fail the job if state is not expected', function(){
        const job = {};
        worker.dequeue.yields(null, job);
        worker.work.yields();
        worker.poll();
        worker.fail.callCount.should.equal(1);
        worker.fail.getCall(0).args[0].should.equal(job);
        worker.fail.getCall(0).args[1].message.should.equal('Unexpected job state');
      });
      it('should correctly set statuses', function(){
        worker.status = 'INIT';
        worker.poll();
        worker.status.should.equal('POLLING');
        worker.dequeue.yield(null, {});
        worker.status.should.equal('WORKING');
        worker.work.yield(null);
        worker.status.should.equal('WORKING');
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
          const m = worker.client.multi();
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
          const m = worker.client.multi();
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
        const jobData = {};
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
        const job = {id: 'id'};
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
          worker.client.multi().lpush.callCount.should.equal(2);
          worker.client.multi().lpush.getCall(0).args[0].should.equal('test_failed');
          worker.client.multi().lpush.getCall(0).args[1].should.equal('jobid');
          worker.client.multi().lpush.getCall(1).args[0].should.equal('jobid_callback');
          worker.client.multi().lpush.getCall(1).args[1].should.equal('jobid');
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
          worker.client.multi().lpush.callCount.should.equal(2);
          worker.client.multi().lpush.getCall(0).args[0].should.equal('test_complete');
          worker.client.multi().lpush.getCall(0).args[1].should.equal('jobid');
          worker.client.multi().lpush.getCall(1).args[0].should.equal('jobid_callback');
          worker.client.multi().lpush.getCall(1).args[1].should.equal('jobid');
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
    describe('throwToBacklog@deprecated', function(){
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
        sinon.stub(worker, 'fail').yieldsAsync(null);
      });
      afterEach(function(){
        worker.fail.restore();
      });
      it('should fail the job if no callback was matched', function(done){
        const job = {name: 'unknown', queue: 'queue'};
        worker.work(job, function(err){
          should.not.exist(err);
          worker.fail.callCount.should.equal(1);
          worker.fail.getCall(0).args[0].should.equal(job);
          worker.fail.getCall(0).args[1].message.should.equal('No callback registered for job queue unknown');
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
          job.status.should.equal('complete');
          done();
        });
      });
      it('should mark job as failed if error exists', function(done){
        cb.yields('failure');
        worker.work(job, function(err){
          err.should.equal('failure');
          job.status.should.equal('failed');
          done();
        });
      });
      it('should error if worker callback is called twice', function(){
        cb.yields(null);
        const callback = sinon.stub();
        worker.work(job, callback);
        callback.callCount.should.equal(1);
        should.not.exist(callback.getCall(0).args[0]);
        cb.yield(null);
        callback.callCount.should.equal(2);
        callback.getCall(1).args[0].message.should.match(/callback is called twice/);
      });
    });
    describe('retry', function(){
      let clock, job;
      beforeEach(function(){
        clock = sinon.useFakeTimers();
        job = {
          id: 'job-id',
          attempt: 1,
          status: 'retry',
          delay: 100500
        };
        sinon.stub(worker, 'fail');
        sinon.stub(worker, 'emit');
        sinon.stub(worker, 'clearTimeout');
        sinon.stub(worker, 'poll');
      });
      afterEach(function(){
        clock.restore();
        worker.fail.restore();
        worker.emit.restore();
        worker.poll.restore();
      });
      it('should set delay, update status, and increment retry count on the job', function(){
        worker.retry(job);
        redis.multi().hmset.callCount.should.equal(1);
        const args = redis.multi().hmset.getCall(0).args;
        args[0].should.equal('job-id');
        args[1].should.eql({
          delay: 100500,
          attempt: 2,
          status: 'retry'
        });
      });
      it('should remove job from progress list', function(){
        worker.retry(job);
        redis.multi().lrem.callCount.should.equal(1);
        const args = redis.multi().lrem.getCall(0).args;
        args[0].should.equal('test_progress');
        args[1].should.equal(0);
        args[2].should.equal('job-id');
      });
      it('should add job id to delay list', function(){
        worker.retry(job);
        redis.multi().lpush.callCount.should.equal(1);
        const args = redis.multi().lpush.getCall(0).args;
        args[0].should.equal('prefix_global-delay');
        args[1].should.equal('job-id');
      });
      it('should fail the job if errors moving the job', function(){
        redis.multi().exec.yields('fail');
        worker.retry(job);
        worker.fail.callCount.should.equal(1);
        const args = worker.fail.getCall(0).args;
        args[0].should.equal(job);
        args[1].should.equal('fail');
      });
      it('should emit retry event', function(){
        redis.multi().exec.yields(null);
        worker.retry(job);
        worker.emit.callCount.should.equal(1);
        const args = worker.emit.getCall(0).args;
        args[0].should.equal('retry');
        args[1].should.equal(job);
      });
      it('should clear job timeout', function(){
        redis.multi().exec.yields(null);
        worker.retry(job);
        worker.clearTimeout.callCount.should.equal(1);
        worker.clearTimeout.getCall(0).args[0].should.equal(job);
      });
      it('should poll for next job', function(){
        redis.multi().exec.yields(null);
        worker.retry(job);
        worker.poll.callCount.should.equal(1);
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