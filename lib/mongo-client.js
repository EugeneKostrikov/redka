'use strict';
var MongoClient = require('mongodb').MongoClient;

exports.connect = function(options, callback){
  MongoClient.connect(options.dburl, function(err, connection){
    if (err) return callback(err);
    connection.collection(options.collectionName || 'redka_jobs', function(err, collection){
      callback(err, collection);
    });
  });
};