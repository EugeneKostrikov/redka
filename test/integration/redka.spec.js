'use strict';
var should = require('should');

module.exports = function(utils){
  describe('Redka', function(){
    var redka;
    beforeEach(function(){
      redka = utils.redka;
    });
    it('should expose status of registered queueues', function(done){
      redka.enqueue('one', 'echo', 1);
      redka.enqueue('source', 'both', 'last');
      redka.enqueue('two', 'error', 1);
      redka.status(function(err, results){
        should.not.exist(err);
        results.should.be.an.Object;
        results.one.progress.should.equal(1);
        results.two.progress.should.equal(1);
        results.source.progress.should.equal(1);
      });
      utils.workers.one.on('complete', function(job){
        if (job.params !== 'last') return;
        redka.status(function(err, results){
          should.not.exist(err);
          results.one.complete.should.equal(2);
          results.two.complete.should.equal(1);
          results.two.failed.should.equal(1);
          results.source.complete.should.equal(1);
          done();
        });
      });
    });
  });
};