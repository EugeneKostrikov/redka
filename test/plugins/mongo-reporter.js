"use strict";
const sinon = require('sinon');
const should = require('should');
const plugin = require('../../plugins/mongo-reporter');
const MongoClient = require('mongodb').MongoClient;
const JobEvents = require('../../lib/job-events');

describe('mongo reporter plugin', function(){
  let receiver, db, reporter;
  beforeEach(function(){
    receiver = {
      onEnqueued: sinon.stub(),
      onDequeued: sinon.stub(),
      onRetry: sinon.stub(),
      onComplete: sinon.stub(),
      onFailed: sinon.stub()
    };
    sinon.stub(JobEvents, 'createReceiver').returns(receiver);

    db = {
      collection: sinon.stub().returnsThis(),
      update: sinon.stub().yields(null)
    };
    sinon.stub(MongoClient, 'connect').yields(null, db);

    reporter = plugin.createReporter({
      redis: 'redis-connection',
      mongodb: 'mongo-connection'
    });
  });
  afterEach(function(){
    JobEvents.createReceiver.restore();
    MongoClient.connect.restore();
  });

  describe('start ',function() {
    beforeEach(function(){
      sinon.stub(reporter, 'write');
    });
    afterEach(function(){
      reporter.write.restore();
    });
    it('should subscribe to redka job events', function(){
      reporter.start();
      JobEvents.createReceiver.callCount.should.equal(1);
      receiver.onEnqueued.callCount.should.equal(1);
      receiver.onDequeued.callCount.should.equal(1);
      receiver.onRetry.callCount.should.equal(1);
      receiver.onComplete.callCount.should.equal(1);
      receiver.onFailed.callCount.should.equal(1);
    });
    it('should open mongo connection', function(){
      reporter.start();
      MongoClient.connect.callCount.should.equal(1);
      MongoClient.connect.getCall(0).args[0].should.equal('mongo-connection');
      db.collection.getCall(0).args[0].should.equal('redka_jobs');
    });
    it('should start writer', function(){
      reporter.start();
      reporter.write.callCount.should.equal(1);
      reporter.started = true;
    });
  });

  describe('stop', function(){
    let clock;
    beforeEach(function(){
      clock = sinon.useFakeTimers();
      reporter.started = true;
    });
    afterEach(function(){
      clock.restore();
    });
    it('should wait for write queue to flush', function(){
      let cb = sinon.stub();
      reporter.writeQueue.push(1);
      reporter.stop(cb);
      cb.callCount.should.equal(0);
      clock.tick(200);
      cb.callCount.should.equal(0);
      reporter.writeQueue.pop();
      clock.tick(100);
      cb.callCount.should.equal(1);
    });
  });

  describe('write', function(){
    let clock;
    beforeEach(function(){
      reporter.collection = db;
      sinon.spy(reporter, 'write');
      clock = sinon.useFakeTimers();
    });
    afterEach(function(){
      reporter.write.restore();
      clock.restore();
    });
    it('should retry in a second if write queue is empty', function(){
      reporter.write();
      db.update.callCount.should.equal(0);
      reporter.write.callCount.should.equal(1);
      clock.tick(1000);
      reporter.write.callCount.should.equal(2);
    });
    it('should send first queue item', function(){
      reporter.writeQueue = [
        {query: 1},
        {query: 2}
      ];
      reporter.write();
      db.update.getCall(0).args[0].should.equal(1);
    });
    it('should set correct .update options', function(){
      reporter.writeQueue.push({query: 'query', update: 'update'});
      reporter.write();
      db.update.callCount.should.equal(1);
      const args = db.update.getCall(0).args;
      args[0].should.equal('query');
      args[1].should.equal('update');
      args[2].should.eql({upsert: true});
    });
    it('should iterate when write completes', function(){
      reporter.writeQueue.push({});
      reporter.write();
      reporter.write.callCount.should.equal(2);
    });
  });
  describe('escapeKeys', function(){
    it('should replace dots with -> in object keys', function(){
      reporter.escapeKeys({
        'one.two.three': 1,
        one: {'two.three': 2},
        array: [{'one.two': 3}]
      }).should.eql({
        'one->two->three': 1,
        one: {'two->three': 2},
        array: [{'one->two': 3}]
      });
    });
    it('should replace heading $ with _ ', function(){
      reporter.escapeKeys({
        $top: 1,
        nested: {$key: 2},
        array: [{$key: 3}]
      }).should.eql({
        _top: 1,
        nested: {_key: 2},
        array: [{_key: 3}]
      });
    });
  });
});