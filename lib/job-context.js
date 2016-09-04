'use strict';
const assert = require('assert');
const _ = require('underscore');

exports.create = function(job, worker){
  const mod = {
    attempt: job.attempt,
    retryIn: retryIn
  };

  function retryIn(delay){
    assert(_.isNumber(delay), 'Delay must be a milliseconds integer value');
    worker.retry(job, delay);
  }

  return mod;
};