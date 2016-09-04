'use strict';
const sinon = require('sinon');
const should = require('should');
const helpers = require('./helpers');

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
      sinon.stub(mongoClient, 'connect');
    });
    afterEach(function(){
      redisClient.initialize.restore();
      mongoClient.connect.restore();
    });
    describe('initialization', function(){
      it('should initialize redis client', function(){
        const opts = {};

        Reporter.create(opts);
        redisClient.initialize.callCount.should.equal(1);
        redisClient.initialize.getCall(0).args[0].should.equal(opts);
      });
      it('should create mongo connection', function(){
        const opts = {};
        Reporter.create({}, opts);
        mongoClient.connect.callCount.should.equal(1);
        mongoClient.connect.getCall(0).args[0].should.equal(opts);
      });
    });
    describe('dummy', function(){
      it('should not open connection to mongo', function(){
        Reporter.dummy({});
        mongoClient.connect.callCount.should.equal(0);
      });
      it('should use noop function in handler', function(){
        const reporter = Reporter.dummy({});
        reporter.mongo.insertOne = sinon.stub();
        reporter.push({id: 'test', serialize: function(){}});
        reporter.mongo.insertOne.callCount.should.equal(1);
      });
    });
    describe('push', function(){
      let clock, reporter;
      beforeEach(function(){
        clock = sinon.useFakeTimers();
        reporter = Reporter.create({}, {}, {});
      });
      afterEach(function(){
        clock.restore();
      });
      it('should queue the job if mongo connection is not open', function(){
        reporter.push({id: 'jobid'});
        reporter.queue.length.should.equal(1);
        reporter.queue[0].should.equal('jobid');
      });
      it('should remove the job from queue and retry insert on timeout', function(){
        reporter.push({id: 'jobid', serialize: function(){ return 'serial'}});
        mongoClient.connect.yield(null, mongo);
        mongo.insertOne.callCount.should.equal(0);
        clock.tick(100);
        mongo.insertOne.callCount.should.equal(1);
        mongo.insertOne.getCall(0).args[0].should.equal('serial');
        reporter.queue.length.should.equal(0);
      });
      it('should push the job to the database immediately if connection is open', function(){
        mongoClient.connect.yield(null, mongo);
        reporter.push({id: 'jobid', serialize: function(){ return 'serial'}});
        mongo.insertOne.callCount.should.equal(1);
        mongo.insertOne.getCall(0).args[0].should.equal('serial');
        reporter.queue.length.should.equal(0);
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
      it('callback if queue length is 0', function(done){
        const reporter = Reporter.dummy({});
        reporter.stop(function(){
          done();
        });
      });
      it('should retry with timeout if queue length is above 0', function(){
        const reporter = Reporter.dummy({});
        const cb = sinon.stub();
        reporter.queue.push(1);
        reporter.stop(cb);
        cb.callCount.should.equal(0);
        clock.tick(100);
        cb.callCount.should.equal(0);
        reporter.queue = [];
        clock.tick(100);
        cb.callCount.should.equal(1);
      });
    });
  });
});