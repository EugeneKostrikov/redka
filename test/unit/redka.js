'use strict';
const sinon = require('sinon');
const should = require('should');

const helpers = require('./helpers');

const Redka = require('../../lib/redka');
const Reporter = require('../../lib/reporter');
const workerModule = require('../../lib/worker');
const Job = require('../../lib/job');
const client = require('../../lib/redis-client');

describe('redka', function(){
  beforeEach(function(){
    sinon.stub(Reporter, 'create').returns('reporter');
    sinon.stub(Reporter, 'dummy').returns('reporter');
  });
  afterEach(function(){
    Reporter.create.restore();
    Reporter.dummy.restore();
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
      redka.reporter.should.equal('reporter');
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
      redka.reporter.should.equal('reporter');
      Reporter.dummy.callCount.should.equal(1);
      Reporter.dummy.getCall(0).args[0].should.equal(opts.redis);
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
    it('should push worker to the reporter', function(){
      redka.options.enableReporting = true;
      redka.reporter = {registerWorker: sinon.stub()};
      redka.worker('q');
      redka.reporter.registerWorker.callCount.should.equal(1);
      redka.reporter.registerWorker.getCall(0).args[0].should.equal(worker);
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
    });
    afterEach(function(){
      Job.create.restore();
      client.initialize.restore();
    });
    it('should set correct queue', function(){
      redka.enqueue('q');
      Job.create.getCall(0).args[0].should.equal('redka_q');
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
    it('should push job id to pending queue', function(done){
      redka.enqueue('queue', 'name', 'params', function(err){
        should.not.exist(err);
        redis.lpush.callCount.should.equal(1);
        redis.lpush.getCall(0).args[0].should.equal('redka_queue_pending');
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
      cb.callCount.should.equal(0);
      redka.removeWorker.yield(null); //fires both
      cb.callCount.should.equal(1);
    });
  });
});