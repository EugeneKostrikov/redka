'use strict';
const _ = require('underscore');
const should = require('should');
const sinon = require('sinon');

const redisClient = require('../../lib/redis-client');
const Callbacks = require('../../lib/callbacks');
const Job = require('../../lib/job');

describe('callbacks', function(){
  let callbacks, redis;
  beforeEach(function(){
    redis = {
      send_command: sinon.stub(),
      hgetall: sinon.stub(),
      brpop: sinon.stub(),
      lpush: sinon.stub(),
      quit: sinon.stub()
    };
    sinon.stub(redisClient, 'initialize').returns(redis);
    callbacks = Callbacks.initialize();
  });
  afterEach(function(){
    redisClient.initialize.restore();
  });
  describe('initialization', function(){
    it('should initialize redis client', function(){
      redisClient.initialize.callCount.should.equal(1);
      callbacks.redis.should.equal(redis);
    });
    it('should set up module props', function(){
      callbacks.status.should.equal('INIT');
      callbacks.callbacks.should.eql({});
      callbacks.lists.should.eql([]);
    });
  });
  describe('poll', function(){
    it('should stop polling if status is STOPPING', function(){
      callbacks.status = 'STOPPING';
      callbacks.poll();
      redis.send_command.callCount.should.equal(0);
    });
    it('should switch status to RUNNING', function(){
      callbacks.poll();
      callbacks.status.should.equal('RUNNING');
    });
    it('should brpop all monitored lists', function(){
      callbacks.lists = ['a','b','c'];
      callbacks.poll();
      redis.send_command.callCount.should.equal(1);
      redis.send_command.getCall(0).args[0].should.equal('brpop');
      redis.send_command.getCall(0).args[1].should.eql(['a','b','c', 10]);
    });
    it('should not send redis command if the monitored list is empty', function(){
      callbacks.poll();
      redis.send_command.callCount.should.equal(0);
      callbacks.lists = ['a'];
      callbacks.poll();
      redis.send_command.callCount.should.equal(1);
    });
    it('should poll again if nothing was found', function(done){
      callbacks.lists.push('ok');
      redis.send_command.yieldsAsync(null, null);
      callbacks.poll();
      sinon.stub(callbacks, 'poll').callsFake(function(){
        callbacks.poll.restore();
        done();
      });
    });
    it('should pass queue name and job data to .handle', function(){
      callbacks.lists.push('list');
      sinon.stub(callbacks, 'handle');
      redis.send_command.yields(null, ['list', 'job']);
      callbacks.poll();
      callbacks.handle.callCount.should.equal(1);
      callbacks.handle.getCall(0).args[0].should.equal('list');
      callbacks.handle.getCall(0).args[1].should.equal('job');
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
      callbacks.stop();
      callbacks.status.should.equal('STOPPING');
    });
    it('should wait for status to get STOPPED and callback', function(){
      let cb = sinon.stub();
      callbacks.stop(cb);
      clock.tick(1000);
      cb.callCount.should.equal(0);
      callbacks.status = 'STOPPED';
      clock.tick(100);
      cb.callCount.should.equal(1);
    });
  });
  describe('handle', function(){
    beforeEach(function(){
      sinon.stub(callbacks, 'poll');
    });
    afterEach(function(){
      callbacks.poll.restore();
    });
    describe('callback exists', function(){
      let cb;
      beforeEach(function(){
        cb = sinon.stub();
        callbacks.callbacks['jobid'] = cb;
        callbacks.lists.push('list');
        sinon.stub(Job, 'create');
      });
      afterEach(function(){
        Job.create.restore();
      });
      it('should resolve job data', function(){
        callbacks.handle('list', 'jobid');
        redis.hgetall.callCount.should.equal(1);
        redis.hgetall.getCall(0).args[0].should.equal('jobid');
      });
      it('should create job object', function(){
        let data = {};
        redis.hgetall.yields(null, data);
        callbacks.handle('list', 'jobid');
        Job.create.callCount.should.equal(1);
        Job.create.getCall(0).args[0].should.equal(data);
      });
      it('should drop stored function and call it with resolved job', function(){
        let data = {}, job = {};
        Job.create.returns(job);
        redis.hgetall.yields(null, data);
        callbacks.handle('list', 'jobid');
        should.not.exist(callbacks.callbacks.jobid);
        cb.callCount.should.equal(1);
        cb.getCall(0).args[1].should.equal(job)
      });
      it('should remove matched list form polling', function(){
        let data = {}, job = {};
        Job.create.returns(job);
        redis.hgetall.yields(null, data);
        callbacks.handle('list', 'jobid');
        callbacks.lists.indexOf('list').should.equal(-1);
      });
      it('should iterate polling', function(){
        let data = {}, job = {};
        Job.create.returns(job);
        redis.hgetall.yields(null, data);
        callbacks.handle('list', 'jobid');
        callbacks.poll.callCount.should.equal(1);
      });
    });
  });
  describe('legacyWaitFor', function(){
    beforeEach(function(){
      sinon.stub(callbacks, 'poll');
    });
    afterEach(function(){
      callbacks.poll.restore();
    });
    it('should store provided job as a callback', function(){
      let cb = function(){};
      callbacks.legacyWaitFor('id', cb);
      callbacks.callbacks.id.should.equal(cb);
    });
    it('should push requested job id to polling list', function(){
      callbacks.legacyWaitFor('id', function(){});
      callbacks.lists.should.eql(['id_callback']);
    });
    it('should kick off polling if status is INIT', function(){
      callbacks.status = 'OTHER';
      callbacks.legacyWaitFor('id', function(){});
      callbacks.poll.callCount.should.equal(0);
      callbacks.status = 'INIT';
      callbacks.legacyWaitFor('id', function(){});
      callbacks.poll.callCount.should.equal(1);
      callbacks.status = 'ANOTHER';
      callbacks.legacyWaitFor('id', function(){});
      callbacks.poll.callCount.should.equal(1);
    })
  });
  describe('waitFor', function(){
    beforeEach(function(){
      redisClient.initialize.resetHistory();
    });
    it('should open new connection', function(){
      callbacks.waitFor('id', function(){});
      redisClient.initialize.callCount.should.equal(1);
      callbacks.waitFor('id', function(){});
      redisClient.initialize.callCount.should.equal(2);
    });
    it('should brpop provided job id', function(){
      callbacks.waitFor('id', function(){});
      redis.brpop.callCount.should.equal(1);
      let args = redis.brpop.getCall(0).args;
      args[0].should.equal('id_callback');
      args[1].should.equal(0);
    });
    it('should fetch job data once brpop calls back', function(){
      let cb = sinon.stub();
      callbacks.waitFor('id', cb);
      redis.brpop.yield(null);
      redis.hgetall.callCount.should.equal(1);
      redis.hgetall.getCall(0).args[0].should.equal('id');
    });
    it('should callback with job instance', function(){
      let cb = sinon.stub();
      callbacks.waitFor('id', cb);
      redis.brpop.yield(null);
      redis.hgetall.yield(null, {});
      cb.callCount.should.equal(1);
      _.isNull(cb.getCall(0).args[0]).should.be.ok;
      should.exist(cb.getCall(0).args[1]);
    });
    it('should close connection', function(){
      let cb = sinon.stub();
      callbacks.waitFor('id', cb);
      redis.brpop.yield(null);
      redis.hgetall.yield(null, {});
      redis.quit.callCount.should.equal(1);
    });
  });
});