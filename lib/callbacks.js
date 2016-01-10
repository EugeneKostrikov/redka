'use strict';
const redisClient = require('./redis-client');
const Job = require('./job');

exports.initialize = function(redisOptions){
  const mod = {
    status: 'INIT',
    redis: redisClient.initialize(redisOptions),
    callbacks: {},
    lists: []
  };

  mod.poll = function(){
    if (this.status === 'STOPPING'){
      this.status = 'STOPPED';
      //TODO: handle outstanding callbacks
      return;
    }
    this.status = 'RUNNING';
    if (this.lists.length === 0) return setTimeout(this.poll.bind(this), 1000);
    let args = this.lists.concat(10);
    this.redis.send_command('brpop', args, (err, item)=>{
      item ? this.handle(item[0], item[1]) : this.poll();
    });
  };

  mod.stop = function(callback){
    this.status = 'STOPPING';
    let int = setInterval(() => {
      if (this.status === 'STOPPED'){
        clearInterval(int);
        callback();
      }
    }, 100);
  };

  mod.handle = function(list, id){
    //No double-callback case is guaranteed by worker
    this.redis.hgetall(id, (err, data) => {
      if (err) return this.handleError(err);
      let j = Job.create(data);
      let fn = this.callbacks[id];
      this.lists.splice(this.lists.indexOf(list), 1);
      delete this.callbacks[id];
      fn(null, j);
    });
    //Sequence does not matter any more
    this.poll();
  };

  mod.handleError = function(err){
    console.error(err);
  };

  mod.waitFor = function(jobId, callback){
    let list = jobId + '_callback';
    let connection = redisClient.initialize(redisOptions);
    connection.brpop(list, 0, function(err){
      if (err) return callback(err);
      connection.hgetall(jobId, (err, data) => {
        if (err) return callback(err);
        let j = Job.create(data);
        connection.quit();
        callback(null, j);
      });
    });
  };

  mod.legacyWaitFor = function(jobId, callback){
    this.callbacks[jobId] = callback;
    let list = jobId + '_callback';
    if (this.lists.indexOf(list) === -1){
      this.lists.push(list);
    }
    if (this.status === 'INIT') this.poll();
  };

  return mod;
};