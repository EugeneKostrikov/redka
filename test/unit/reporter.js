'use strict';
const sinon = require('sinon');
const should = require('should');
const helpers = require('./helpers');

const Job = require('../../lib/job');
const redisClient = require('../../lib/redis-client');
const mongoClient = require('../../lib/mongo-client');
const Reporter = require('../../lib/reporter');

describe('reporters', function(){
  describe('mongodb', function(){
    let redis, mongo;
    beforeEach(function(){
      redis = helpers.mockRedis();
      redis.send_command = sinon.stub();
      mongo = {insertOne: sinon.stub()};
      sinon.stub(redisClient, 'initialize').returns(redis);
      sinon.stub(mongoClient, 'connect').yields(null, mongo);
    });
    afterEach(function(){
      redisClient.initialize.restore();
      mongoClient.connect.restore();
    });
    describe('initialization', function(){
      it('should initialize redis client', function(){
        let opts = {};

        Reporter.create(opts);
        redisClient.initialize.callCount.should.equal(1);
        redisClient.initialize.getCall(0).args[0].should.equal(opts);
      });
      it('should create mongo connection', function(){
        let opts = {};
        Reporter.create({}, opts);
        mongoClient.connect.callCount.should.equal(1);
        mongoClient.connect.getCall(0).args[0].should.equal(opts);
      });
      it('should start polling for jobs', function(){
        redis.send_command.called.should.not.be.ok;
        Reporter.create();
        redis.send_command.called.should.be.ok;
      });
    });
    describe('registerWorker', function(){
      let reporter, worker;
      beforeEach(function(){
        reporter = Reporter.create();
        worker = {
          lists: {
            complete: 'complete-list',
            failed: 'failed-list'
          }
        };
      });
      it('should extend monitored lists with complete queue', function(){
        reporter.registerWorker(worker);
        reporter.lists.should.containEql('complete-list');
      });
      it('should extend monitored lists with failed queue', function(){
        reporter.registerWorker(worker);
        reporter.lists.should.containEql('failed-list');
      });
    });
    describe('poll', function(){
      let reporter;
      beforeEach(function(){
        reporter = Reporter.create({}, {});
        redis.send_command = sinon.stub();
        sinon.stub(reporter, 'handle');
        sinon.spy(reporter, 'poll');
        reporter.registerWorker({lists: {complete: 'complete', failed: 'failed'}});
      });
      afterEach(function(){
        reporter.handle.restore();
        reporter.poll.restore();
      });
      it('should submit all monitored lists', function(){
        reporter.poll();
        redis.send_command.callCount.should.equal(1);
        redis.send_command.getCall(0).args[0].should.equal('brpop');
        redis.send_command.getCall(0).args[1].should.eql(['complete', 'failed', 1]);
      });
      it('should handle result if polling returned something', function(){
        reporter.poll();
        redis.send_command.yield(null, [null, 'result']);
        reporter.handle.callCount.should.equal(1);
        reporter.handle.getCall(0).args[0].should.equal('result');
      });
      it('should poll again if result is empty', function(){
        reporter.poll();
        redis.send_command.yield(null, null);
        reporter.poll.callCount.should.equal(2);
      });
      it('should not poll if worker status is STOPPING', function(){
        reporter.status = 'STOPPING';
        reporter.poll();
        redis.send_command.callCount.should.equal(0);
        reporter.status.should.equal('STOPPED');
      });
    });
    describe('handle', function(){
      let reporter, job;
      beforeEach(function(){
        reporter = Reporter.create({}, {});
        job = {
          serialize: sinon.stub().returns('serialized')
        };
        sinon.stub(reporter, 'poll');
        sinon.stub(Job, 'create').returns(job);
      });
      afterEach(function(){
        reporter.poll.restore();
        Job.create.restore();
      });
      it('should fetch job data', function(){
        reporter.handle('key');
        redis.hgetall.callCount.should.equal(1);
        redis.hgetall.getCall(0).args[0].should.equal('key');
      });
      it('should initialize the job', function(){
        let data = {};
        redis.hgetall.yields(null, data);
        reporter.handle('key');
        Job.create.callCount.should.equal(1);
        Job.create.getCall(0).args[0].should.equal(data);
      });
      it('should push serialized version into mongo', function(){
        let data = {};
        redis.hgetall.yields(null, data);
        mongo.insertOne.yields(null);
        reporter.handle('key');
        mongo.insertOne.callCount.should.equal(1);
        mongo.insertOne.getCall(0).args[0].should.equal('serialized');
      });
      it('should drop handled job', function(){
        let data = {};
        redis.hgetall.yields(null, data);
        mongo.insertOne.yields(null);
        redis.del.yields(null);
        reporter.handle('key');
        redis.del.callCount.should.equal(1);
        redis.del.getCall(0).args[0].should.equal('key');
      });
      it('should poll for the next job', function(){
        let data = {};
        redis.hgetall.yields(null, data);
        mongo.insertOne.yields(null);
        redis.del.yields(null);
        reporter.handle('key');
        reporter.poll.callCount.should.equal(1);
      });
    });
    describe('dummy', function(){
      it('should not open connection to mongo', function(){
        let reporter = Reporter.dummy({});
        should.not.exist(reporter.mongo);
      });
      it('should use noop function in handler', function(){
        redis.hgetall.yields(null, {});
        let reporter = Reporter.dummy({});
        sinon.spy(reporter, 'noopinsert');
        reporter.handle('test');
        reporter.noopinsert.callCount.should.equal(1);
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
      it('should set status to STOPPING', function(){
        let reporter = Reporter.dummy({});
        reporter.status.should.equal('RUNNING');
        reporter.stop();
        reporter.status.should.equal('STOPPING');
      });
      it('should callback once status is STOPPED', function(){
        let reporter = Reporter.dummy({});
        let cb = sinon.stub();
        reporter.stop(cb);
        reporter.status.should.equal('STOPPING');
        cb.callCount.should.equal(0);
        clock.tick(100);
        cb.callCount.should.equal(0);
        reporter.status = 'STOPPED';
        clock.tick(100);
        cb.callCount.should.equal(1);
      });
    });
  });
});