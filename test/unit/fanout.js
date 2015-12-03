'use strict';
var fanout = require('../../lib/fanout');
var should = require('should');
var sinon = require('sinon');

describe('/lib/fanout', function(){
  describe('init', function(){
    it('should pull the worker for required source queue', function(){
      let worker = {register: sinon.stub()};
      let redka = {worker: sinon.stub().returns(worker)};
      fanout.init(redka, 'testing', {});
      redka.worker.calledOnce.should.be.ok;
    });
    it('should generate fanout handlers for all provided jobs', function(){
      let worker = {register: sinon.stub()};
      let redka = {worker: sinon.stub().returns(worker)};
      fanout.init(redka, 'testing', {
        resource: [{queue: 'a', name: 'b'}],
        other: [{queue: 'a', name: 'b'}]
      });
      let args = worker.register.getCall(0).args[0];
      args.resource.should.be.a.Function;
      args.other.should.be.a.Function;
      args.should.have.keys(['resource', 'other']);
    });
    it('should register the callbacks with the worker', function(){
      let worker = {register: sinon.stub()};
      let redka = {worker: sinon.stub().returns(worker)};
      fanout.init(redka, 'testing', {
        resource: [{queue: 'a', name: 'b'}],
        other: [{queue: 'a', name: 'b'}]
      });
      worker.register.calledOnce.should.be.ok;
    });
  });

  describe('fanout', function(){
    it('should generate worker handler', function(){
      let result = fanout.fanout();
      result.should.be.a.Function;
      result.length.should.equal(2);
    });
    it('should enqueue received job to destination queue + name pair', function(done){
      let redka = {enqueue: sinon.stub()};
      let destinations = [{queue: 'q', name: 'n'}];
      fanout.fanout(redka, destinations)({}, function(err){
        should.not.exist(err);
        redka.enqueue.getCall(0).args.should.eql(['q', 'n', {}]);
        done();
      });
    });
    it('should iterate over all destinations provided', function(done){
      let redka = {enqueue: sinon.stub()};
      let destinations = [
        {queue: 1, name: 1},
        {queue: 2, name: 2},
        {queue: 3, name: 3}
      ];
      fanout.fanout(redka, destinations)({}, function(err){
        should.not.exist(err);
        redka.enqueue.callCount.should.equal(3);
        done();
      });
    });
  });
});