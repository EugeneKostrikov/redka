'use strict';
var MongoClient = require('mongodb').MongoClient;

module.exports = function(options, callback){
  MongoClient.connect(options.dburl, function(err, connection){
    if (err) return callback(err);
    connection.collection(options.collectionName || 'redka-jobs', function(err, collection){
      callback(err, collection);
    });
  });
};