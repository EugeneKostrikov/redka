'use strict';
var should =  require('should');

module.exports = function(utils){
  var redka;
  describe('worker', function(){
    beforeEach(function(){
      redka = utils.redka;
    });
    it('should fail errored job', function(done){
      var w = utils.workers.one;
      redka.enqueue('one', 'error', 1);
      w.once('failed', function(){
        redka.status(function(err, results){
          should.not.exist(err);
          results.one.failed.should.equal(1);
          done();
        });
      });
    });
    it('should complete successful jobs', function(done){
      var w = utils.workers.one;
      w.once('complete', function(){
        redka.status(function(err, results){
          should.not.exist(err);
          results.one.complete.should.equal(1);
          done();
        });
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
