'use strict';

var Redka = require('../lib/redka');
describe('Redka', function(){
  var utils = {};
  before(function(){
    var redka = new Redka({
      redis: {
        host: '127.0.0.1',
        port: 6379
      }
    });

    var workerOne = redka.worker('one');
    var workerTwo = redka.worker('two');
    utils.workers = {
      one: workerOne,
      two: workerTwo
    };

    workerOne.register({
      echo: function(data, callback){
        callback(null, data);
      },
      error: function(data, callback){
        callback(data);
      },
      fanout: function(data, callback){
        callback(data);
      }
    });

    workerTwo.register({
      echo: function(data, callback){
        callback(null, data);
      },
      error: function(data, callback){
        callback(data);
      },
      fanout: function(data,callback){
        callback(data);
      }
    });

    redka.fanout('source', {
      one: [
        {queue: 'one', name: 'echo'}
      ],
      two: [
        {queue: 'two', name: 'echo'}
      ],
      both: [
        {queue: 'one', name: 'echo'},
        {queue: 'two', name: 'echo'}
      ]
    });

    redka.start();

    utils.redka = redka;

  });

  require('./integration/worker.spec')(utils);
  require('./integration/fanout.spec')(utils);
  require('./integration/redka.spec')(utils);

  afterEach(function(done){
    utils.redka._reset(function(err){
      utils.workers.one.removeAllListeners('complete');
      utils.workers.one.removeAllListeners('failed');
      utils.workers.one.removeAllListeners('enqueued');
      utils.workers.one.removeAllListeners('dequeued');
      utils.workers.two.removeAllListeners('complete');
      utils.workers.two.removeAllListeners('failed');
      utils.workers.two.removeAllListeners('enqueued');
      utils.workers.two.removeAllListeners('dequeued');
      done(err);
    });
  });

});

