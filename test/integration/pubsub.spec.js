'use strict';
var should = require('should');

module.exports = function(utils){
  describe('pusub interface', function(){
    var redka, channel;
    beforeEach(function(){
      redka = utils.redka;
      channel = redka.getPubsubChannel();
    });
    it('should publish events', function(){
      (function(){
        channel.publish('testing', 'test');
      }).should.not.throw();
    });
    it('should subscribe to events', function(done){
      channel.subscribe('testing', function(channel, data){
        data.should.equal('testing');
      });
      channel.on('message', function(channel, data){
        channel.should.equal('testing');
        data.should.equal('test');
        done();
      });
      channel.publish('testing', 'test');
    });
  });
};