"use strict";
const async = require('async');

function WorkerMultiplex(){
  this._workers = [];
}
WorkerMultiplex.prototype._addWorker = function(worker){
  this._workers.push(worker);
};
WorkerMultiplex.prototype.register = function(...args){
  this._workers.forEach(worker => worker.register(...args));
};
WorkerMultiplex.prototype.stop = function(callback){
  async.each(this._workers, (worker, next) => worker.stop(next), callback);
};

exports.WorkerMultiplex = WorkerMultiplex;
exports.create = function(...args){
  return new WorkerMultiplex(...args);
};