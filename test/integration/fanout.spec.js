'use strict';

module.exports = function(utils){
  describe('fanout', function(){
    var redka, one, two;
    beforeEach(function(){
      redka = utils.redka;
      one = utils.workers.one;
      two = utils.workers.two;
    });
    afterEach(function(){
      one.removeAllListeners('complete');
      two.removeAllListeners('complete');
    });
    it('should pick job from source queue and clone to destination queue', function(done){
      one.once('complete', function(job){
        job.params.should.equal('fanout');
        done();
      });
      redka.enqueue('source', 'one', 'fanout');
    });
    it('should be able to route jobs to different queues by job name', function(done){
      var onecount = 0, twocount = 0;
      function trydone(){
        if (onecount > 0 && twocount > 0){
          onecount.should.equal(1);
          twocount.should.equal(1);
          done();
        }
      }
      function oneListener(job){
        job.params.should.equal('fanout to one');
        onecount++;
        trydone();
      }
      function twoListener(job){
        job.params.should.equal('fanout to two');
        twocount++;
        trydone();
      }
      one.on('complete', oneListener);
      two.on('complete', twoListener);
      redka.enqueue('source', 'one', 'fanout to one');
      redka.enqueue('source', 'two', 'fanout to two');
    });
    it('should push cloned jobs to all queues provided with configuration', function(done){
      var oc = false, tc = false;
      one.once('complete', function(job){
        job.params.should.equal('fanout to both');
        oc = true;
        if (tc) done();
      });
      two.once('complete', function(job){
        job.params.should.equal('fanout to both');
        tc = true;
        if (oc) done();
      });
      redka.enqueue('source', 'both', 'fanout to both');
    });
    it('should not fail to clone valid jobs if one of destinations is invalid queue', function(done){
        done();
    });
  });
};