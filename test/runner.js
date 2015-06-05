'use strict';

var Redka = require('../lib/redka');
describe('Redka', function(){
  var utils = {};
  before(function(done){
    var redka = new Redka({
      redis: {
        host: process.env.REDIS_PORT_6379_TCP_ADDR || '127.0.0.1',
        port: process.env.REDIS_PORT_6379_TCP_PORT || 6379
      },
      enableReporting: true,
      mongodb: {
        dburl: (process.env.CI ?
          'mongodb://' + process.env.MONGO_PORT_27017_TCP_ADDR + ':' + process.env.MONGO_PORT_27017_TCP_PORT :
          'mongodb://localhost:27017') + '/redka-test',
        collectionName: 'redka-jobs'
      }
    });

    var workerOne = redka.worker('one', {timeout: 100});
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
      },
      timeout: function(data ,callback){}
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

    redka.onReady(function(){
      utils.mongo = redka.mongodb;
      done();
    });
  });

  require('./integration/worker.spec')(utils);
  require('./integration/fanout.spec')(utils);
  require('./integration/redka.spec')(utils);
  require('./integration/pubsub.spec')(utils);
  require('./integration/reporter.spec')(utils);

  afterEach(function(done){
      utils.redka._reset(function(err){
        utils.workers.one.removeAllListeners('complete');
        utils.workers.one.removeAllListeners('failed');
        utils.workers.one.removeAllListeners('enqueued');
        utils.workers.one.removeAllListeners('dequeued');
        utils.workers.one.removeAllListeners('timeout');
        utils.workers.two.removeAllListeners('complete');
        utils.workers.two.removeAllListeners('failed');
        utils.workers.two.removeAllListeners('enqueued');
        utils.workers.two.removeAllListeners('dequeued');
        utils.workers.two.removeAllListeners('timeout');
        done(err);
      });
  });

});

