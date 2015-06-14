'use strict';
var should =  require('should');

module.exports = function(utils){
  var redka, mongo;
  describe('worker', function(){
    beforeEach(function(){
      redka = utils.redka;
      mongo = utils.mongo;
    });
    it('should fail errored job', function(done){
      var w = utils.workers.one;
      redka.enqueue('one', 'error', 1);
      w.once('failed', function(){
        setTimeout(function(){
          mongo.find({status: 'failed'}).toArray(function(err, results){
            should.not.exist(err);
            results.length.should.equal(1);
            done();
          });
        }, 10);
      });
    });
    it('should complete successful jobs', function(done){
      var w = utils.workers.one;
      w.once('complete', function(){
        setTimeout(function(){
          mongo.find({status: 'complete'}).toArray(function(err, results){
            should.not.exist(err);
            results.length.should.equal(1);
            done();
          });
        }, 10);
      });
      redka.enqueue('one', 'echo', 1);
    });
    it('should not block other queues waiting for new jobs', function(done){
      var wtwo = redka._workers.redka_two;
      var count = 0;
      var listener = function(){
        count++;
        if (count === 2) {
          wtwo.removeListener('complete', listener);
          done();
        }
      };
      wtwo.on('complete', listener);
      redka.enqueue('one', 'echo', 1);
      redka.enqueue('two', 'echo', 1);
      redka.enqueue('two', 'echo', 1);
    });
    it('should timeout stuck jobs', function(done){
      var w = utils.workers.one;
      w.on('timeout', function(){
        done();
      });
      redka.enqueue('one', 'timeout', 1);
    });
    it('should support objects in job params', function(done){
      var w = utils.workers.one;
      w.once('complete', function(){
        setTimeout(function(){
          mongo.findOne({status: 'complete'}, function(err, job){
            should.not.exist(err);
            job.params.should.be.an.Object;
            job.params.one.two.should.equal(3);
            job.result.should.equal('3');
            done();
          });
        }, 10);
      });
      redka.enqueue('one', 'object', {one: {two: 3}});
    });
    it('should fail job if callback is called twice', function(done){
      var w = utils.workers.one;
      w.once('complete', function(){
        setTimeout(function(){
          mongo.findOne({}, function(err, job){
            should.not.exist(err);
            job.status.should.equal('failed');
            job.error.should.equal('Error: Job callback is called twice');
            done();
          });
        }, 10);
      });
      redka.enqueue('one', 'dblcb', 1);
    });
    it.skip('should fail job if callback is called twice asynchronously with no delay', function(done){
      var w = utils.workers.one;
      //w.once('complete', function(){
        setTimeout(function(){
          redka.status(function(err, stats){
            console.log('queue status ', stats);
          mongo.findOne({status: 'failed'}, function(err, job){
            should.not.exist(err);
            job.status.should.equal('failed');
            job.error.should.equal('Error: Job callback is called twice');
            done();
          });
          });
        }, 100);
      //});
      redka.enqueue('one', 'dblcbAsync', 1);
    });
  });
};
