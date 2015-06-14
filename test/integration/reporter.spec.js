'use strict';
var should = require('should');

module.exports = function(utils){

  describe('reporter', function(){
    var mongo, redka, w1, w2;
    beforeEach(function(){
      mongo = utils.mongo;
      redka = utils.redka;
      w1 = utils.workers.one;
      w2 = utils.workers.two;
    });

    it('should insert job document when worker it is processed', function(done){
      redka.enqueue('one', 'echo', 1);
      //w1.once('complete', function(){
        setTimeout(function(){
          mongo.find({}).toArray(function(err, data){
            should.not.exist(err);
            data.length.should.greaterThan(0);
            done();
          });
        }, 10);
      //});
    });

  });
};