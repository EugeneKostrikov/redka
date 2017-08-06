'use strict';
const sinon = require('sinon');
const should = require('should');

const helpers = require('./helpers');

const Redka = require('../../lib/redka');
const workerModule = require('../../lib/worker');
const Job = require('../../lib/job');
const client = require('../../lib/redis-client');
const Callbacks = require('../../lib/callbacks');
const DelayedJobsManager = require('../../lib/delayed-jobs-manager');
const JobEvents = require('../../lib/job-events');

describe('redka', function(){
  let callbacks, delayedJobsManager, jobEventEmitter, jobEventReceiver;
  beforeEach(function(){
    jobEventEmitter = {
      jobEnqueued: sinon.stub()
    };
    jobEventReceiver = {
      onComplete: sinon.stub().returns(function(){}),
      onFailed: sinon.stub().returns(function(){})
    };
    callbacks = {
      waitFor: sinon.stub()
    };
    delayedJobsManager = {
      start: sinon.stub(),
      stop: sinon.stub()
    };
    sinon.stub(Callbacks, 'initialize').returns(callbacks);
    sinon.stub(DelayedJobsManager, 'create').returns(delayedJobsManager);
    sinon.stub(JobEvents, 'createEmitter').returns(jobEventEmitter);
    sinon.stub(JobEvents, 'createReceiver').returns(jobEventReceiver);
  });
  afterEach(function(){
    Callbacks.initialize.restore();
    DelayedJobsManager.create.restore();
    JobEvents.createEmitter.restore();
    JobEvents.createReceiver.restore();
  });
  describe('initialization', function(){
    beforeEach(function(){
      sinon.stub(client, 'initialize').returns('client');
    });
    afterEach(function(){
      client.initialize.restore();
    });
    it('should initialise two redis clients', function(){
      let opts = {redis: {}};
      let redka = new Redka(opts);
      redka.client.should.equal('client');
      redka.subscribeClient.should.equal('client');
      client.initialize.callCount.should.equal(2);
      client.initialize.getCall(0).args[0].should.equal(opts.redis);
      client.initialize.getCall(1).args[0].should.equal(opts.redis);
    });
    it('should be able to set custom prefix and default to redka_ prefix', function(){
      let redka = new Redka({});
      redka.prefix.should.equal('redka_');
      let other = new Redka({prefix: 'custom_'});
      other.prefix.should.equal('custom_');
    });
    it('should start delayed jobs manager passing in correct options if configured to', function(){
      let opts = {
        redis: {},
        runDelayedJobsManager: true,
        delayOptions: {}
      };
      let redka = new Redka(opts);
      redka.delayedJobsManager.should.equal(delayedJobsManager);
      DelayedJobsManager.create.callCount.should.equal(1);
      const args = DelayedJobsManager.create.getCall(0).args;
      args[0].should.equal(opts.redis);
      args[1].should.equal(opts.delayOptions);
      delayedJobsManager.start.callCount.should.equal(1);
    });
    it('should not start delayed jobs manager if option is not provided explicitly', function(){
      let opts = {
        redis: {},
        delayOptions: {}
      };
      let redka = new Redka(opts);
      DelayedJobsManager.create.callCount.should.equal(0);
    });
  });

  describe('worker', function(){
    let redis, redka, worker;
    beforeEach(function(){
      redis = helpers.mockRedis();
      worker = {};
      sinon.stub(client, 'initialize').returns(redis);
      sinon.stub(workerModule, 'create').returns(worker);
      redka = new Redka({redis: 'redis opts'});
    });
    afterEach(function(){
      client.initialize.restore();
      workerModule.create.restore();
    });
    it('should set correct queue', function(){
      redka.worker('queue');
      workerModule.create.getCall(0).args[0].should.equal('redka_queue');
    });
    it('should error if the queue is duplicate (two workers listening on single queue)', function(){
      redka._workers.redka_queue = true;
      (function(){
        redka.worker('queue');
      }).should.throw(/duplicate worker/);
    });
    it('should initialize worker', function(){
      let wopts = {};
      redka.worker('queue', wopts);
      workerModule.create.callCount.should.equal(1);
      workerModule.create.getCall(0).args[0].should.equal('redka_queue');
      workerModule.create.getCall(0).args[1].should.equal('redis opts');
      workerModule.create.getCall(0).args[2].should.equal(wopts);
    });
    it('should init multiple workers if parallel option provided', function(){
      redka.worker('queue', {parallel: 5});
      workerModule.create.callCount.should.equal(5);
      redka._workers.redka_queue.length.should.equal(5);
    });
    it('should keep track of created workers', function(){
      redka.worker('q');
      should.exist(redka._workers.redka_q);
      redka._workers.redka_q.should.eql([worker]);
    });
    it('should return worker multiplex', function(){
      const multiplex = redka.worker('q');
      multiplex.register.should.be.a.Function();
    });
  });
  describe('enqueue', function(){
    let job, redis, redka;
    beforeEach(function(){
      job = {id: 'jobid', serialize: sinon.stub().returns('serialized')};
      sinon.stub(Job, 'create').returns(job);
      redis = helpers.mockRedis();
      sinon.stub(client, 'initialize').returns(redis);
      redka = new Redka({});
    });
    afterEach(function(){
      Job.create.restore();
      client.initialize.restore();
    });
    it('should set correct queue', function(){
      redka.enqueue('q');
      Job.create.getCall(0).args[0].should.equal('redka_q');
    });
    it('should throw if queue name is system-reserved', function(){
      (function(){
        redka.enqueue('_global-delay');
      }).should.throw('_global-delay is redka reserved queue name');
    });
    it('should initialize new job', function(){
      redka.enqueue('queue', 'name', 'params');
      Job.create.callCount.should.equal(1);
      Job.create.getCall(0).args[0].should.equal('redka_queue');
      Job.create.getCall(0).args[1].should.equal('name');
      Job.create.getCall(0).args[2].should.equal('params');
    });
    it('should hmset serialised job data', function(done){
      redis.hmset.yields('stop');
      redka.enqueue('queue', 'name', 'params', function(err){
        err.should.equal('stop');
        redis.hmset.callCount.should.equal(1);
        redis.hmset.getCall(0).args[0].should.equal('jobid');
        redis.hmset.getCall(0).args[1].should.equal('serialized');
        done();
      });
    });
    it('should push job id to pending queue if delay option is not provided', function(done){
      redis.lpush.yields('stop');
      redka.enqueue('queue', 'name', 'params', function(err){
        err.should.equal('stop');
        redis.lpush.callCount.should.equal(1);
        redis.lpush.getCall(0).args[0].should.equal('redka_queue_pending');
        redis.lpush.getCall(0).args[1].should.equal('jobid');
        done();
      });
    });
    it('should push job id to pending queue if delay option is 0', function(done){
      job.delay = 0;
      redis.lpush.yields('stop');
      redka.enqueue('queue', 'name', 'params', {delay: 0}, function(err){
        err.should.equal('stop');
        redis.lpush.callCount.should.equal(1);
        redis.lpush.getCall(0).args[0].should.equal('redka_queue_pending');
        redis.lpush.getCall(0).args[1].should.equal('jobid');
        done();
      });
    });
    it('should push job id to delay queue if delay option is set', function(done){
      job.delay = 1;
      redis.lpush.yields('stop');
      redka.enqueue('queue', 'name', 'params', {delay: 1}, function(err){
        err.should.equal('stop');
        redis.lpush.callCount.should.equal(1);
        redis.lpush.getCall(0).args[0].should.equal('redka__global-delay');
        redis.lpush.getCall(0).args[1].should.equal('jobid');
        done();
      });
    });
    it('should immediately callback on possible error', function(done){
      redis.lpush.yieldsAsync('stop');
      redka.enqueue('queue', 'name', 'params', function(err){
        err.should.equal('stop');
        redis.lpush.callCount.should.equal(1);
        done();
      });
    });
    it('should emit job enqueued event', function(done){
      redka.enqueue('queue', 'name', 'params', function(){
        jobEventEmitter.jobEnqueued.callCount.should.equal(1);
        jobEventEmitter.jobEnqueued.getCall(0).args[0].should.equal(job);
        done();
      });
      redka.jobEventReceiver.onComplete.yield({job: {id: 'jobid'}});
    });
    it('should callback with job error when FAILED event is emitted for matching job', function(done){
      redka.enqueue('queue', 'name', 'params', function(err, result){
        err.should.be.instanceOf(Error);
        err.message.should.equal('err');
        should.not.exist(result);
        done();
      });
      redka.jobEventReceiver.onFailed.yield({job: {id: 'jobid', status: 'failed', error: 'err'}});
    });
    it('should callback with job result when COMPLETE event is emitted for matching job', function(done){
      redka.enqueue('queue', 'name', 'params', function(err, result){
        should.not.exist(err);
        result.should.equal('result');
        done();
      });
      redka.jobEventReceiver.onComplete.yield({job: {id: 'jobid', result: 'result'}});
    });
    it('should ignore events for other jobs', function(){
      const callback = sinon.stub();
      redka.enqueue('queue', 'name', 'params', callback);
      redka.jobEventReceiver.onComplete.yield({job: {id: 'other-job-id'}});
      callback.callCount.should.equal(0);
      redka.jobEventReceiver.onFailed.yield({job: {id: 'other-job-id'}});
      callback.callCount.should.equal(0);
    });
    it('should unsubscribe from job events when either of events fired for matchign job', function(done){
      const completeUnsub = sinon.stub();
      redka.jobEventReceiver.onComplete.returns(completeUnsub);
      const failedUnsub = sinon.stub();
      redka.jobEventReceiver.onFailed.returns(failedUnsub);
      redka.enqueue('queue', 'name', 'params', function(){
        completeUnsub.callCount.should.equal(1);
        failedUnsub.callCount.should.equal(1);
        done();
      });
      redka.jobEventReceiver.onComplete.yield({job: {id: 'jobid'}});
    });
  });
  describe('removeWorker', function(){
    let redka, redis;
    beforeEach(function(){
      redis = helpers.mockRedis();
      sinon.stub(client, 'initialize').returns(redis);
      redka = new Redka({redis: {}});
    });
    afterEach(function(){
      client.initialize.restore();
    });
    it('should callback with error if provided queue has no running worker', function(done){
      redka.removeWorker('something-missing', function(err){
        err.message.should.match(/no worker registered/);
        done();
      });
    });
    it('should callback once worker is stopped', function(done){
      const w = {stop: sinon.stub().yieldsAsync()};
      redka._workers['redka_testing'] = [w];
      redka.removeWorker('testing', function(err){
        should.not.exist(err);
        w.stop.callCount.should.equal(1);
        done();
      });
    });
    it('should drop worker reference once worker is stopped', function(done){
      const w = {stop: sinon.stub().yieldsAsync()};
      redka._workers['redka_testing'] = [w];
      redka.removeWorker('testing', function(err){
        should.not.exist(err);
        should.not.exist(redka._workers['redka_testing']);
        done();
      });
    });
  });
  describe('stop', function(){
    let redis, redka;
    beforeEach(function(){
      redis = helpers.mockRedis();
      sinon.stub(client, 'initialize').returns(redis);
      redka = new Redka({redis: {}, runDelayedJobsManager: true});
      sinon.stub(redka, 'removeWorker');
    });
    afterEach(function(){
      client.initialize.restore();
      redka.removeWorker.restore();
    });
    it('should pass correct queue names to removeWorker', function(){
      const cb = sinon.stub();
      redka.worker('one');
      redka.worker('two');
      redka.stop(cb);
      redka.removeWorker.getCall(0).args[0].should.equal('one');
      redka.removeWorker.getCall(1).args[0].should.equal('two');
    });
    it('should remove all running workers', function(){
      const cb = sinon.stub();
      redka.worker('one');
      redka.worker('two');
      redka.stop(cb);
      redka.removeWorker.callCount.should.equal(2);
    });
    it('should stop delayed jobs manager', function(){
      const cb = sinon.stub();
      redka.stop(cb);
      redka.delayedJobsManager.stop.callCount.should.equal(1);
    });
    it('should wait for all components to stop', function(){
      const cb = sinon.stub();
      redka.worker('one');
      redka.worker('two');
      redka.stop(cb);
      cb.callCount.should.equal(0);
      redka.removeWorker.yield(null); //Fire both workers
      cb.callCount.should.equal(0);
      redka.delayedJobsManager.stop.yield(null); //Fire delay
      cb.callCount.should.equal(1);
    });
  });
});