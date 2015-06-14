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
      setTimeout(function(){
      redka.status(function(err, results){
        should.not.exist(err);
        results.should.be.an.Object;
        results.one.should.have.keys(['pending', 'progress', 'failed', 'complete']);
        results.two.should.have.keys(['pending', 'progress', 'failed', 'complete']);
        results.source.should.have.keys(['pending', 'progress', 'failed', 'complete']);
        done();
      });
      }, 10);
    });
  });
};