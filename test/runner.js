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

  });

  require('./integration/worker.spec')(utils);
  require('./integration/fanout.spec')(utils);
  require('./integration/redka.spec')(utils);
  require('./integration/pubsub.spec')(utils);

  afterEach(function(done){
    function drain(callback){
      setTimeout(function(){
        utils.redka.status(function(err, res){
          if (err) return callback(err);
          var complete = true;
          console.log('queue status ', res);
          Object.keys(res).forEach(function(wname){
            if (res[wname].pending !== 0 || res[wname].progress !== 0) complete = false;
          });
          console.log('is complete ', complete);
          complete ? callback() : drain(callback);
        });
      }, 10);
    }
   // drain(function(err){
   //   if (err) console.error(err);
   //   if (err) return done(err);
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
    //});
  });

});

