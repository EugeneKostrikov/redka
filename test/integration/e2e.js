'use strict';
const async = require('async');
const should = require('should');
const Redka = require('../../lib/redka');

describe('E2E flow', function(){
  this.timeout(20000);
  let redka;
  beforeEach(function(){
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
    let worker = redka.worker('testing');
    worker.register({
      ok: function(data, cb) {cb(null, data)},
      fail: function(data, cb) {cb(data);}
    });
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
});