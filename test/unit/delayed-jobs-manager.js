'use strict';
const sinon = require('sinon');
const should = require('should');
const redisClient = require('../../lib/redis-client');
const helpers = require('./helpers');

const DelayedJobsManager = require('../../lib/delayed-jobs-manager');

describe('DelayedJobsManager', function(){
  let clock;
  let client, service;
  beforeEach(function(){
    clock = sinon.useFakeTimers();
    client = helpers.mockRedis();
    client.script.yields(null, 'sha');
    sinon.stub(redisClient, 'initialize').returns(client);
    service = DelayedJobsManager.create();
  });
  afterEach(function(){
    clock.restore();
    redisClient.initialize.restore();
  });
  describe('initialization', function(){
    it('should initialize redis client', function(){
      redisClient.initialize.callCount.should.equal(1);
    });
  });
  describe('start', function(){
    it('should load the scripts to redis server', function(){
      service.start();
      client.script.callCount.should.equal(2);
      const args = client.script.getCall(0).args;
      args[0].should.equal('load');
      args[1].should.be.of.type('string');
    });
    it('should set interval to run the script', function(){
      service.start();
      service.intervals.length.should.equal(2);
    });
    it('should kick off script by id passing in current date', function(){
      service.start();

      client.evalsha.callCount.should.equal(0);
      clock.tick(100);
      client.evalsha.callCount.should.equal(2);
      clock.tick(100);
      client.evalsha.callCount.should.equal(4);

      const firstCall = client.evalsha.getCall(0).args;
      firstCall[0].should.equal('sha');
      firstCall[1].should.equal(0);
      firstCall[2].should.equal(100); //mock date

      const secondCall = client.evalsha.getCall(2).args;
      secondCall[0].should.equal('sha');
      secondCall[1].should.equal(0);
      secondCall[2].should.equal(200); //mock date
    });
  });
  describe('stop', function(){
    beforeEach(function(){
      service.start();
    });
    it('should clear the interval', function(done){
      service.stop(function(){
        clock.tick(200);
        client.evalsha.callCount.should.equal(0);
        done();
      });
    });
    it('should callback', function(done){
      service.stop(function(){
        done();
      });
    });
  });
});