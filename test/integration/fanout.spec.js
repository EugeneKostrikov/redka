'use strict';
var _ = require('underscore');
var should = require('should');

module.exports = function(utils){
  describe('fanout', function(){
    var redka, mongo, one, two;
    beforeEach(function(){
      redka = utils.redka;
      mongo = utils.mongo;
      one = utils.workers.one;
      two = utils.workers.two;
    });
    it('should pick job from source queue and clone to destination queue', function(done){
      redka.enqueue('source', 'one', 'fanout');
      setTimeout(function(){
        mongo.find({}).toArray(function(err, jobs){
          should.not.exist(err);
          var source = _.find(jobs, function(job){
            return job.queue === 'redka_source' && job.name === 'one';
          });
          var fanout = _.find(jobs, function(job){
            return job.queue === 'redka_one' && job.name === 'echo';
          });
          should.exist(source);
          should.exist(fanout);
          jobs.length.should.equal(2);
          done();
        });
      }, 10);
    });
    it('should be able to route jobs to different queues by job name', function(done){
      redka.enqueue('source', 'one', 'fanout to one');
      redka.enqueue('source', 'two', 'fanout to two');
      setTimeout(function(){
        mongo.find({}).toArray(function(err, jobs){
          should.not.exist(err);
          var sourceOne = _.find(jobs, function(job){
            return job.queue === 'redka_source' && job.name === 'one';
          });
          var sourceTwo = _.find(jobs, function(job){
            return job.queue === 'redka_source' && job.name === 'two';
          });
          should.exist(sourceOne);
          should.exist(sourceTwo);
          var fanoutOne = _.find(jobs, function(job){
            return job.queue === 'redka_one' && job.name === 'echo';
          });
          var fanoutTwo = _.find(jobs, function(job){
            return job.queue === 'redka_two' && job.name === 'echo';
          });
          should.exist(fanoutOne);
          should.exist(fanoutTwo);
          jobs.length.should.equal(4);
          done();
        });
      }, 10);
    });
    it('should push cloned jobs to all queues provided with configuration', function(done){
      redka.enqueue('source', 'both', 'fanout to both');
      setTimeout(function(){
        mongo.find({}).toArray(function(err, jobs){
          should.not.exist(err);
          jobs.length.should.equal(3);
          done();
        });
      }, 10);
    });
  });
};