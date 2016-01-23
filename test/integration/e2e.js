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
      mongodb: {
        dburl: (process.env.CI ?
        'mongodb://' + process.env.MONGO_PORT_27017_TCP_ADDR + ':' + process.env.MONGO_PORT_27017_TCP_PORT :
          'mongodb://localhost:27017') + '/redka-test',
        collectionName: 'redka-jobs'
      }
    });
    worker = redka.worker('testing', {parallel: 10, spinup: 10});
    worker.register({
      delay: function(data, cb) { setTimeout(cb, 1000)},
      ok: function(data, cb) {cb(null, data)},
      fail: function(data, cb) {cb(data);}
    });
    let int = setInterval(function(){
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
      redka.stop(done);
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
    async.parallel([1,2,3,4,5,6,7,8,9,10].map(function(){
      return function(cb){
        let start = Date.now();
        redka.enqueue('testing', 'delay', 'data', function(err){
          should.not.exist(err);
          cb(null, Date.now() - start);
        });
      }
    }), function(err, results){
      should.not.exist(err);
      //300ms is more than enough for transactional overhead
      _.last(results).should.be.lessThan(1300);
      done();
    });
  });
});