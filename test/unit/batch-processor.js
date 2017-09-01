"use strict";
require('should');
const sinon = require('sinon');
const redisClient = require('../../lib/redis-client');
const batcher = require('../../lib/batch-processor');

describe('batch processor', function(){
  let client, worker;
  beforeEach(function(){
    client = {
      script: sinon.stub().yields(null, 'script-sha'),
      evalsha: sinon.stub().yields(null)
    };
    sinon.stub(redisClient, 'initialize').returns(client);
    worker = batcher.makeBatcher({connect: 1});
  });
  afterEach(function(){
    redisClient.initialize.restore();
  });
  describe('init', function(){
    it('should open redis connection', function(){
      redisClient.initialize.callCount.should.equal(1);
      redisClient.initialize.getCall(0).args[0].should.eql({connect: 1});
    });
    it('should set status to initializing', function(){
      worker.running.should.equal(false);
      worker.batches.should.eql([]);
    });
  });
  describe('start', function(){
    beforeEach(function(){
      sinon.stub(worker, 'eval');
    });
    afterEach(function(){
      worker.eval.restore();
    });
    it('should toggle worker running status', function(){
      worker.start();
      worker.running.should.equal(true);
    });
    it('should upload batcher script', function(){
      worker.start();
      client.script.callCount.should.equal(1);
      const args = client.script.getCall(0).args;
      args[0].should.equal('load');
    });
    it('should kick off evaluation providing script sha', function(){
      worker.start();
      worker.eval.callCount.should.equal(1);
      worker.eval.getCall(0).args[0].should.equal('script-sha')
    });
  });
  describe('add', function(){
    it('should store provided config', function(){
      worker.add({
        sourceQueue: 'source-queue',
        sourceName: 'job-name',
        targetQueue: 'target-queue',
        targetName: 'job-name',
        batchInterval: 2000,
        batchSize: 10
      });
      worker.batches.length.should.equal(1);
      worker.batches[0].batchInterval.should.equal(2000);
    });
    it('should default batch interval to 1 second', function(){
      worker.add({
        sourceQueue: 'source-queue',
        sourceName: 'job-name',
        targetQueue: 'target-queue',
        targetName: 'job-name',
        batchSize: 1
      });
      worker.batches[0].batchInterval.should.equal(1000);
    });
    it('should validate provided options', function(){
      (function(){
        worker.add({});
      }).should.throw('sourceQueue option is required');
      (function(){
        worker.add({sourceQueue: 'source'});
      }).should.throw('sourceName option is required');
      (function(){
        worker.add({sourceQueue: 1, sourceName: 1});
      }).should.throw('targetQueue option is required');
      (function(){
        worker.add({sourceQueue: 1, sourceName: 1, targetQueue: 1});
      }).should.throw('targetName option is required');
      (function(){
        worker.add({sourceQueue: 1, sourceName: 1, targetQueue: 1, targetName: 1});
      }).should.throw('batchSize option is required');
    });
  });
  describe('eval', function(){
    let clock;
    beforeEach(function(){
      clock = sinon.useFakeTimers(1234);
      sinon.stub(worker, 'generateJobId').returns('job-id');
      worker.add({
        sourceQueue: 'source-queue',
        sourceName: 'source-job-name',
        targetQueue: 'target-queue',
        targetName: 'target-job-name',
        batchInterval: 1000,
        batchSize: 10
      });
      worker.running = true;
    });
    afterEach(function(){
      clock.restore();
      worker.generateJobId.restore();
    });
    it('should iterate over all configured batches', function(){
      worker.add({
        sourceQueue: 'source-two',
        sourceName: 'source-job-name',
        targetQueue: 'target-two',
        targetName: 'target-job-name',
        batchInterval: 2000,
        batchSize: 20
      });
      worker.eval('sha');
      client.evalsha.callCount.should.equal(2);
      const first = client.evalsha.getCall(0).args;
      first[0].should.equal('sha');
      first[1].should.equal(4);
      first[2].should.equal('redka_source-queue');
      first[3].should.equal('source-job-name');
      first[4].should.equal('redka_target-queue');
      first[5].should.equal('target-job-name');
      first[6].should.equal(1000);
      first[7].should.equal(10);
      first[8].should.equal('job-id');
      first[9].should.equal(1234);
      first[10].should.equal('1970-01-01T00:00:01.234Z');

      const second = client.evalsha.getCall(1).args;
      second[0].should.equal('sha');
      second[1].should.equal(4);
      second[2].should.equal('redka_source-two');
      second[3].should.equal('source-job-name');
      second[4].should.equal('redka_target-two');
      second[5].should.equal('target-job-name');
      second[6].should.equal(2000);
      second[7].should.equal(20);
      second[8].should.equal('job-id');
      second[9].should.equal(1234);
      second[10].should.equal('1970-01-01T00:00:01.234Z');
    });
    it('should iterate in 200ms', function(){
      client.evalsha.callCount.should.equal(0);
      worker.eval('sha');
      client.evalsha.callCount.should.equal(1);
      clock.tick(200);
      client.evalsha.callCount.should.equal(2);
    });
    it('should stop iteration if processor status is stopping', function(){
      worker.running = false;
      worker.eval();
      client.evalsha.callCount.should.equal(0);
    });
  });
  describe('stop', function(){
    it('should toggle processor status to stopping',function(){
      worker.running = true;
      worker.stop();
      worker.running.should.equal(false);
    });
  });
});