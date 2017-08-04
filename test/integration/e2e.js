'use strict';
const async = require('async');
const _ = require('underscore');
const should = require('should');
const Redka = require('../../lib/redka');

describe('E2E flow', function(){
  this.timeout(200000);
  let redka, worker;
  beforeEach(function(done){
    redka = new Redka({
      redis: {
        host: process.env.REDIS_PORT_6379_TCP_ADDR || '127.0.0.1',
        port: process.env.REDIS_PORT_6379_TCP_PORT || 6379
      },
      enableReporting: true,
      runDelayedJobsManager: true,
      mongodb: {
        dburl: (process.env.CI ?
        'mongodb://' + process.env.MONGO_PORT_27017_TCP_ADDR + ':' + process.env.MONGO_PORT_27017_TCP_PORT :
          'mongodb://localhost:27017') + '/redka-test',
        collectionName: 'redka-jobs'
      }
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
    });
    const int = setInterval(function(){
      if (redka.reporter.mongo){
        clearInterval(int);
        redka.reporter.mongo.deleteMany({}, function(){
          done();
        });
      }
    }, 100);
  });
  afterEach(function(done){
    redka.reporter.mongo.deleteMany({}, function(){
      redka.stop(function(){
        console.log('stop callback fired');
        done();
      });
    });
  });
  it('should handle successful job and mark it as complete', function(done){
    redka.enqueue('testing', 'ok', 'data');
    setTimeout(function(){
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
        },
        function(cb){
          redka.reporter.mongo.find({status: 'complete'}).toArray(function(err, res){
            should.not.exist(err);
            res.length.should.equal(1);
            cb();
          });
        }
      ], done);
    }, 100);
  });
  it('should handle throwing jobs and mark it as failed', function(done){
    redka.enqueue('testing', 'fail', 'data');
    setTimeout(function(){
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
        },
        function(cb){
          redka.reporter.mongo.find({status: 'failed'}).toArray(function(err, res){
            should.not.exist(err);
            res.length.should.equal(1);
            cb();
          });
        }
      ], done);
    }, 100);
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
});