'use strict';
const Redka = require('./lib/redka');

exports.makeRedkaClient = function(options){
  return new Redka(options);
};