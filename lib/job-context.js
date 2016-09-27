'use strict';
const assert = require('assert');
const _ = require('underscore');

exports.create = function(job, workcallback){
  const mod = {
    attempt: job.attempt,
    retryIn: retryIn,
    note: note,
    look: look
  };

  function retryIn(delay){
    assert(_.isNumber(delay), 'Delay must be a milliseconds integer value');
    job.retry(delay);
    workcallback(null);
  }

  function note(key, val){
    job.note(key, val);
  }

  function look(key){
    return job.look(key);
  }


  return mod;
};