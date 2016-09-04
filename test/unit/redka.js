'use strict';
const sinon = require('sinon');
const should = require('should');

const helpers = require('./helpers');

const Redka = require('../../lib/redka');
const Reporter = require('../../lib/reporter');
const workerModule = require('../../lib/worker');
const Job = require('../../lib/job');
const client = require('../../lib/redis-client');
const Callbacks = require('../../lib/callbacks');
const Destructor = require('../../lib/destructor');
const DelayedJobsManager = require('../../lib/delayed-jobs-manager');

describe('redka', function(){
  let reporter, callbacks, destructor, delayedJobsManager;
  beforeEach(function(){
    callbacks = {
      waitFor: sinon.stub()
    };
    reporter = {
      stop: sinon.stub(),
      push: sinon.stub()
    };
    destructor = {
      drain: sinon.stub()
    };
    delayedJobsManager = {
      start: sinon.stub(),
      stop: sinon.stub()
    };
    sinon.stub(Reporter, 'create').returns(reporter);
    sinon.stub(Reporter, 'dummy').returns(reporter);
    sinon.stub(Callbacks, 'initialize').returns(callbacks);
    sinon.stub(Destructor, 'initialize').returns(destructor);
    sinon.stub(DelayedJobsManager, 'create').returns(delayedJobsManager);
  });
  afterEach(function(){
    Reporter.create.restore();
    Reporter.dummy.restore();
    Callbacks.initialize.restore();
    Destructor.initialize.restore();
    DelayedJobsManager.create.restore();
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
    it('should start mongo reporter if reporting is enabled', function(){
      let opts  = {
        redis: {},
        mongodb: {},
        enableReporting: true,
        reportingOptions: {}
      };
      let redka = new Redka(opts);
      redka.reporter.should.equal(reporter);
      Reporter.create.callCount.should.equal(1);
      Reporter.create.getCall(0).args[0].should.equal(opts.redis);
      Reporter.create.getCall(0).args[1].should.equal(opts.mongodb);
      Reporter.create.getCall(0).args[2].should.equal(opts.reportingOptions);
    });
    it('should start dummy reporter if reporting is disabled', function(){
      let opts  = {
        redis: {}
      };
      let redka = new Redka(opts);
      redka.reporter.should.equal(reporter);
      Reporter.dummy.callCount.should.equal(1);
      Reporter.dummy.getCall(0).args[0].should.equal(opts.redis);
    });
    it('should start delayed jobs manager passing in correct options', function(){
      let opts = {
        redis: {},
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
    it('should keep track of created workers', function(){
      redka.worker('q');
      should.exist(redka._workers.redka_q);
      redka._workers.redka_q.should.equal(worker);
    });
    it('should return created worker', function(){
      redka.worker('q').should.equal(worker);
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
      redka.jobCallbacks.waitFor.yields(null, {status: 'complete'});
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
      redis.hmset.yieldsAsync('stop');
      redka.enqueue('queue', 'name', 'params', function(err){
        err.should.equal('stop');
        redis.hmset.callCount.should.equal(1);
        redis.hmset.getCall(0).args[0].should.equal('jobid');
        redis.hmset.getCall(0).args[1].should.equal('serialized');
        done();
      });
    });
    it('should push job id to pending queue if delay option is not provided', function(done){
      redka.enqueue('queue', 'name', 'params', function(err){
        should.not.exist(err);
        redis.lpush.callCount.should.equal(1);
        redis.lpush.getCall(0).args[0].should.equal('redka_queue_pending');
        redis.lpush.getCall(0).args[1].should.equal('jobid');
        done();
      });
    });
    it('should push job id to pending queue if delay option is 0', function(done){
      job.delay = 0;
      redka.enqueue('queue', 'name', 'params', {delay: 0}, function(err){
        should.not.exist(err);
        redis.lpush.callCount.should.equal(1);
        redis.lpush.getCall(0).args[0].should.equal('redka_queue_pending');
        redis.lpush.getCall(0).args[1].should.equal('jobid');
        done();
      });
    });
    it('should push job id to delay queue if delay option is set', function(done){
      job.delay = 1;
      redka.enqueue('queue', 'name', 'params', {delay: 1}, function(err){
        should.not.exist(err);
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
    it('should register the callback if it was provided', function(done){
      redka.jobCallbacks.waitFor.yields(null, {status: 'complete'});
      redka.enqueue('queue', 'name', 'params', function(){
        redka.jobCallbacks.waitFor.callCount.should.equal(1);
        redka.jobCallbacks.waitFor.getCall(0).args.length.should.equal(2);
        redka.jobCallbacks.waitFor.getCall(0).args[0].should.equal('jobid');
        done();
      });
    });
    it('should callback with job error when job status is failed', function(done){
      redka.jobCallbacks.waitFor.yieldsAsync(null, {status: 'failed', error: 'err'});
      redka.enqueue('queue', 'name', 'params', function(err, result){
        err.should.be.instanceOf(Error);
        err.message.should.equal('err');
        should.not.exist(result);
        done();
      });
    });
    it('should callback with job result when job status is compete', function(done){
      redka.jobCallbacks.waitFor.yieldsAsync(null, {status: 'complete', result: 'result'});
      redka.enqueue('queue', 'name', 'params', function(err, result){
        should.not.exist(err);
        result.should.equal('result');
        done();
      });
    });
    it('should callback with unexpected state error if job status is unknown', function(done){
      redka.jobCallbacks.waitFor.yieldsAsync(null, {status: 'unknown'});
      redka.enqueue('queue', 'name', 'params', function(err, result){
        err.message.should.equal('Unexpected job status');
        should.not.exist(result);
        done();
      });
    });
    it('should push completed job to reporter', function(done){
      let job = {status: 'complete', result: 'result'};
      redka.jobCallbacks.waitFor.yieldsAsync(null, job);
      redka.enqueue('queue', 'name', 'params', function(){
        redka.reporter.push.callCount.should.equal(1);
        redka.reporter.push.getCall(0).args[0].should.equal(job);
        done();
      });
    });
    it('should drain the job with destructor', function(done){
      let job = {id: 'id', status: 'complete', result: 'result'};
      redka.jobCallbacks.waitFor.yieldsAsync(null, job);
      redka.enqueue('queue', 'name', 'params', function(){
        redka.destructor.drain.callCount.should.equal(1);
        redka.destructor.drain.getCall(0).args[0].should.equal(job);
        done();
      });
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
      let w = {stop: sinon.stub().yieldsAsync()};
      redka._workers['redka_testing'] = w;
      redka.removeWorker('testing', function(err){
        should.not.exist(err);
        w.stop.callCount.should.equal(1);
        done();
      });
    });
    it('should drop worker reference once worker is stopped', function(done){
      let w = {stop: sinon.stub().yieldsAsync()};
      redka._workers['redka_testing'] = w;
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
      redka = new Redka({redis: {}});
      sinon.stub(redka, 'removeWorker');
    });
    afterEach(function(){
      client.initialize.restore();
      redka.removeWorker.restore();
    });
    it('should pass correct queue names to removeWorker', function(){
      let cb = sinon.stub();
      redka.worker('one');
      redka.worker('two');
      redka.stop(cb);
      redka.removeWorker.getCall(0).args[0].should.equal('one');
      redka.removeWorker.getCall(1).args[0].should.equal('two');
    });
    it('should remove all running workers', function(){
      let cb = sinon.stub();
      redka.worker('one');
      redka.worker('two');
      redka.stop(cb);
      redka.removeWorker.callCount.should.equal(2);
    });
    it('should stop reporter', function(){
      let cb = sinon.stub();
      redka.stop(cb);
      redka.reporter.stop.callCount.should.equal(1);
    });
    it('should stop delayed jobs manager', function(){
      let cb = sinon.stub();
      redka.stop(cb);
      redka.delayedJobsManager.stop.callCount.should.equal(1);
    });
    it('should wait for all components to stop', function(){
      let cb = sinon.stub();
      redka.worker('one');
      redka.worker('two');
      redka.stop(cb);
      cb.callCount.should.equal(0);
      redka.removeWorker.yield(null); //Fire both workers
      cb.callCount.should.equal(0);
      redka.reporter.stop.yield(null); //Fire reporter
      cb.callCount.should.equal(0);
      redka.delayedJobsManager.stop.yield(null); //Fire delay
      cb.callCount.should.equal(1);
    });
  });
});