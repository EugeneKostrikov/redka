"use strict";
const _ = require('underscore');
const sinon = require('sinon');
const should = require('should');
const RedisClient = require('../../lib/redis-client');
const Job = require('../../lib/job');
const JobEvents = require('../../lib/job-events');


describe('job-events', function(){
  let redisClient;
  beforeEach(function(){
    redisClient = {
      publish: sinon.stub()
    };
    sinon.stub(RedisClient, 'initialize').returns(redisClient);
  });
  afterEach(function(){
    RedisClient.initialize.restore();
  });
  describe('Emitter', function(){
    let emitter, job, clock;
    beforeEach(function() {
      job = {serialize: sinon.stub().returns('serial-job')};
      emitter = JobEvents.createEmitter({});
      clock = sinon.useFakeTimers(1501927916204);
    });
    afterEach(function(){
      clock.restore();
    });
    it('should create redis client', function(){
      RedisClient.initialize.callCount.should.equal(1);
    });
    describe('event', function(){
      it('should return proper event type', function(){
        emitter.event('TYPE', job).type.should.equal('TYPE');
      });
      it('should return string date', function(){
        emitter.event('TYPE', job).timestamp.should.equal('2017-08-05T10:11:56.204Z');
      });
      it('should return serialised job', function(){
        emitter.event('TYPE', job).job.should.equal('serial-job');
      });
    });
    describe('emitEvent', function(){
      let event;
      beforeEach(function(){
        event = {type: 'EVENT_TYPE'};
      });
      it('should publish into correct channel', function(){
        emitter.emitEvent(event);
        redisClient.publish.callCount.should.equal(1);
        redisClient.publish.getCall(0).args[0].should.equal('redka-job-events:EVENT_TYPE');
      });
      it('should send event payload as JSON string', function(){
        emitter.emitEvent(event);
        redisClient.publish.getCall(0).args[1].should.equal('{"type":"EVENT_TYPE"}');
      });
    });
    describe('publish methods', function(){
      beforeEach(function(){
        sinon.stub(emitter, 'event').returns('event-structure');
        sinon.stub(emitter, 'emitEvent');
      });
      afterEach(function(){
        emitter.event.restore();
        emitter.emitEvent.restore();
      });
      it('should provide job event methods', function(){
        emitter.jobEnqueued.should.be.a.Function();
        emitter.jobDequeued.should.be.a.Function();
        emitter.jobRetry.should.be.a.Function();
        emitter.jobComplete.should.be.a.Function();
        emitter.jobFailed.should.be.a.Function();
      });
      it('should create and emit event for provided job with correct event type', function(){
        const job = {};

        emitter.jobEnqueued(job);
        emitter.event.callCount.should.equal(1);
        emitter.event.getCall(0).args[0].should.equal('ENQUEUED');
        emitter.event.getCall(0).args[1].should.equal(job);

        emitter.emitEvent.callCount.should.equal(1);
        emitter.emitEvent.getCall(0).args[0].should.equal('event-structure');
      });
    });

  });
  describe('Receiver', function(){
    let receiver, messageCallback;
    beforeEach(function(){
      redisClient.subscribe = sinon.stub().returnsThis();
      redisClient.on = sinon.stub();

      receiver = JobEvents.createReceiver();
      messageCallback = redisClient.on.getCall(0).args[1];
      sinon.stub(Job, 'create').returns({id: 'job-id'});
    });
    afterEach(function(){
      Job.create.restore();
    });
    it('should subscribe to channels', function(){
      redisClient.subscribe.callCount.should.equal(5);
    });
    describe('subscribeTo', function(){
      it('should put callback into correct channel bucket', function(){
        const stub = sinon.stub();
        receiver.onEnqueued(stub);
        _.values(receiver._callbacks['redka-job-events:ENQUEUED'])[0].should.equal(stub);
      });
      it('should parse message and job content', function(){
        const stub = sinon.stub();
        receiver.onEnqueued(stub);
        const expected = {
          type: 'ENQUEUED',
          job: {id: 'job-id'}
        };
        messageCallback('redka-job-events:ENQUEUED', JSON.stringify(expected));
        Job.create.callCount.should.equal(1);
        Job.create.getCall(0).args[0].should.eql({'id': 'job-id'});
        stub.callCount.should.equal(1);
        stub.getCall(0).args[0].should.eql(expected);
      });
      it('should return unsubscribe function', function(){
        const stubOne = sinon.stub();
        const unsubOne = receiver.onEnqueued(stubOne);
        const stubTwo = sinon.stub();
        receiver.onEnqueued(stubTwo);

        messageCallback('redka-job-events:ENQUEUED', JSON.stringify({}));
        stubOne.callCount.should.equal(1);
        stubTwo.callCount.should.equal(1);

        unsubOne();
        messageCallback('redka-job-events:ENQUEUED', JSON.stringify({}));
        stubOne.callCount.should.equal(1);
        stubTwo.callCount.should.equal(2);
      });
    });
  });
});