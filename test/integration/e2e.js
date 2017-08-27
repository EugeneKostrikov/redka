'use strict';
const async = require('async');
const _ = require('underscore');
const should = require('should');
const Redka = require('../../lib/redka');

describe('E2E flow', function(){
  this.timeout(200000);
  let redka, worker;
  beforeEach(function(){
    redka = new Redka({
      redis: {
        host: process.env.REDIS_PORT_6379_TCP_ADDR || '127.0.0.1',
        port: process.env.REDIS_PORT_6379_TCP_PORT || 6379
      },
      runDelayedJobsManager: true
    });
    worker = redka.worker('testing', {parallel: 10});
    worker.register({
      delay: function(data, cb) { setTimeout(cb, 1000)},
      ok: function(data, cb) {cb(null, data)},
      time: function(data, cb){ cb(null, Date.now())},
      fail: function(data, cb) {cb(data);},
      retry: function(data, cb){
        if (this.attempt < 3) return this.retryIn(500);
        if (data.fail) {
          return cb(new Error('Max retries reached'));
        }
        cb(null, Date.now());
      },
      stuck: function(data, cb){
        if (this.attempt === 2) return cb(null, this.attempt);
        setTimeout(() => {
          cb(null, this.attempt);
        }, 500); //still flush first attempt
      }
    });
  });
  afterEach(function(done){
    redka.stop(function(){
      done();
    });
  });
  it('should handle successful job and mark it as complete', function(done){
    redka.enqueue('testing', 'ok', 'data', function(err, results){
      should.not.exist(err);
      async.parallel([
        function(cb){
          redka.client.lrange('redka_testing_pending', 0, -1, function(err, res){
            should.not.exist(err);
            res.length.should.equal(0);
            cb();
          });
        },
        function(cb){
          redka.client.lrange('redka_testing_progress', 0, -1, function(err, res){
            should.not.exist(err);
            res.length.should.equal(0);
            cb();
          });
        }
      ], done);
    });
  });
  it('should handle throwing jobs and mark it as failed', function(done){
    redka.enqueue('testing', 'fail', 'data', function(err){
      should.exist(err);
      async.parallel([
        function(cb){
          redka.client.lrange('redka_testing_pending', 0, -1, function(err, res){
            should.not.exist(err);
            res.length.should.equal(0);
            cb();
          });
        },
        function(cb){
          redka.client.lrange('redka_testing_progress', 0, -1, function(err, res){
            should.not.exist(err);
            res.length.should.equal(0);
            cb();
          });
        }
      ], done);
    });
  });
  it('should delete the job from database when job is complete', function(done){
    redka.enqueue('testing', 'ok', 'data', function(err){
      should.not.exist(err);
      setTimeout(function(){
        redka.client.keys('*', function(err, keys){
          should.not.exist(err);
          keys.length.should.equal(0);
          done();
        });
      }, 100);
    });
  });
  it('should be able to callback when job is complete', function(done){
    redka.enqueue('testing', 'ok', 'data', function(error, result){
      should.not.exist(error);
      result.should.equal('data');
      setTimeout(function(){
        done();
      }, 100);
    });
  });
  it('should pass parsed data to complete callback if parseable', function(done){
    redka.enqueue('testing', 'ok', {a: {b: {c: 1}}}, function(error, result){
      should.not.exist(error);
      result.should.eql({a: {b: {c: 1}}});
      setTimeout(function(){
        done();
      });
    });
  });
  it('should pass through job error if the job fails', function(done){
    redka.enqueue('testing', 'fail', 'data', function(error, result){
      error.should.be.instanceOf(Error);
      error.message.should.equal('data');
      should.not.exist(result);
      setTimeout(function(){
        done();
      }, 100);
    });
  });
  it('should correctly handle parallel option', function(done){
    async.parallel(_.range(10).map(function(){
      return function(cb){
        let start = Date.now();
        redka.enqueue('testing', 'delay', 'data', function(err){
          should.not.exist(err);
          cb(null, Date.now() - start);
        });
      }
    }), function(err, results){
      should.not.exist(err);
      //100ms is more than enough for transactional overhead
      _.last(results).should.be.lessThan(1100);
      done();
    });
  });
  it('should be able to run delayed jobs with milliseconds delay', function(done){
    const start = Date.now();
    redka.enqueue('testing', 'time', 'data', {delay: 1000}, function(err, end){
      should.not.exist(err);
      //100ms for transactional overhead

      (end - (start + 1000)).should.be.approximately(100, 100);
      done();
    });
  });
  it('should be able to run delayed job with date-like delay', function(done){
    const execWhen = new Date(Date.now() + 1000);
    _.isDate(execWhen).should.equal(true);
    redka.enqueue('testing', 'time', 'data', {delay: execWhen}, function(err, end){
      should.not.exist(err);
      //100ms for transactional overhead
      new Date(end).should.be.greaterThan(execWhen);
      done();
    });
  });
  it('should be able to schedule job retry from handler', function(done){
    const startedAt = Date.now();
    redka.enqueue('testing', 'retry', {fail: false}, function(err, endedAt){
      should.not.exist(err);
      (endedAt - (startedAt + 1000)).should.be.approximately(100, 100);
      done();
    });
  });
  it('should provide the number of retries to handler (allow failing on many attempts)', function(done){
    redka.enqueue('testing', 'retry', {fail: true}, function(err){
      err.message.should.equal('Max retries reached');
      done();
    });
  });
  it('should be able to stop the worker after a number of retries fired', function(done){
    //Just adding the jobs here. The afterEach step should not get stuck
    redka.enqueue('testing', 'retry', {fail: false});
    redka.enqueue('testing', 'retry', {fail: false});
    redka.enqueue('testing', 'retry', {fail: false});
    setTimeout(function(){
      done();
    }, 2000); //Let it iterate couple times
  });
  it('should put job back into queue when it hands in _progress queue with staled heartbeat value', function(done){
    redka.enqueue('testing', 'stuck', {}, function(err){
      should.not.exist(err);
      done();
    });
    const staledHeartbeat = Date.now() - 30000;
    function getJobId(callback) {
      redka.client.lrange('redka_testing_progress', 0, -1, (err, list) => {
        if (err) return callback(err);
        if (list.length > 0) return callback(null, list[0]);
        setTimeout(() => {
          getJobId(callback);
        }, 100);
      });
    }
    getJobId((err, jobId) => {
      should.not.exist(err);
      should.exist(jobId);
      redka.client.hset(jobId, 'heartbeat', staledHeartbeat, (err, writeResult) => {
        should.not.exist(err);
      });
    });
  });
});