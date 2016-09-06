'use strict';
const sinon = require('sinon');
const should = require('should');
const helpers = require('./helpers');

const Job = require('../../lib/job');
const redisClient = require('../../lib/redis-client');

const Worker = require('../../lib/worker');

describe('worker running parallel jobs', function(){
  describe('polling', function() {
    let redis, worker, cb, clock;
    beforeEach(function () {
      redis = helpers.mockRedis();
      sinon.stub(redisClient, 'initialize').returns(redis);
      worker = Worker.create('test', {}, {
        timeout: 100,
        parallel: 3
      });
      sinon.spy(worker, 'poll');

      sinon.stub(worker, 'dequeue');
      sinon.stub(worker, 'work');
      sinon.stub(worker, 'complete', function () {
        worker.poll();
      });

      cb = sinon.stub();
      clock = sinon.useFakeTimers();
    });
    afterEach(function () {
      redisClient.initialize.restore();
      worker.poll.restore();
      worker.dequeue.restore();
      worker.work.restore();
      worker.complete.restore();
      clock.restore();
    });
    it('should be able to kick off job handlers in parallel', function () {
      worker.dequeue.yields(null, {status: 'complete'});
      worker.register({test: cb});
      worker.poll.callCount.should.equal(3);
      worker.dequeue.callCount.should.equal(3);
      worker.work.callCount.should.equal(3);
    });
    it('should be able to limit the number of jobs running parallel', function () {
      worker.dequeue.yields(null, {status: 'complete'});
      worker.register({test: cb});
      worker.poll.callCount.should.equal(3);

      worker.work.getCall(0).yield(null);
      worker.complete.callCount.should.equal(1);
      worker.poll.callCount.should.equal(4);

      worker.work.getCall(1).yield(null);
      worker.complete.callCount.should.equal(2);
      worker.poll.callCount.should.equal(5);
    });
    it('should keep correct worker status when stopped', function () {
      worker.register({test: cb});
      worker.runningCount.should.equal(0);
      worker.dequeue.getCall(0).yield(null, {status: 'complete'});
      worker.runningCount.should.equal(1);
      worker.status.should.equal('WORKING');

      worker.dequeue.getCall(1).yield(null, {status: 'complete'});
      worker.runningCount.should.equal(2);
      worker.status.should.equal('WORKING');

      const stopcb = sinon.stub();
      worker.stop(stopcb);
      worker.status.should.equal('STOPPING');

      //dequeue yields no job when status is STOPPING
      worker.dequeue.getCall(2).yield(null, null);
      worker.runningCount.should.equal(2);
      worker.status.should.equal('STOPPING');

      worker.work.getCall(0).yield(null);
      worker.runningCount.should.equal(1);
      worker.status.should.equal('STOPPING');

      worker.work.getCall(1).yield(null);
      worker.runningCount.should.equal(0);
      worker.status.should.equal('STOPPED');

      clock.tick(100);
      stopcb.called.should.be.ok;
    });
  });
  describe('dequeue', function(){
    let redis, worker, clock;
    beforeEach(function(){
      redis = helpers.mockRedis();
      redis.brpoplpush = sinon.stub();
      sinon.stub(redisClient, 'initialize').returns(redis);
      sinon.stub(Job, 'create');

      worker = Worker.create('test', {}, {
        timeout: 100,
        parallel: 3
      });

      sinon.stub(worker, 'timeout');

      clock = sinon.useFakeTimers();
    });
    afterEach(function(){
      redisClient.initialize.restore && redisClient.initialize.restore();
      Job.create.restore && Job.create.restore();
      worker.timeout.restore && worker.timeout.restore();
      clock.restore && clock.restore();
    });
    it('should send brpoplpush if no command is pending now', function(){
      const cb = sinon.stub();
      worker.dequeue(cb);
      worker.dequeue(cb);
      redis.brpoplpush.callCount.should.equal(1);
    });
    it('should retry brpoplpush after a timeout', function(){
      const cb = sinon.stub();
      worker.dequeue(cb);
      worker.dequeue(cb);
      redis.brpoplpush.callCount.should.equal(1);
      sinon.spy(worker, 'dequeue');
      worker.dequeue.callCount.should.equal(0);
      clock.tick(500);
      worker.dequeue.callCount.should.equal(1);
      worker.dequeue.restore(); //clean up
    });
    it('should clear pending state as soon as callback is fired', function(){
      worker.pendingDequeue.should.equal(false);
      const cb = sinon.stub();
      worker.dequeue(cb);
      worker.dequeue(cb);
      worker.pendingDequeue.should.equal(true);

      redis.brpoplpush.getCall(0).yield(null, {});
      worker.pendingDequeue.should.equal(false);
    });
  });
});