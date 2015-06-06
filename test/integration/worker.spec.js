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
  });
};
