"use strict";
const Redka = require('../index.js');
const assert = require('assert');
const client = new Redka({});

const server = new Redka({});

const worker = server.worker('hello');
let fanoutCounter = 0;

worker.register({
  world: (data, cb) => {
    fanoutCounter++;
    cb(null, `Hello world`);
  },
  user: (data, cb) => {
    fanoutCounter++;
    cb(null, `Hello ${data.username}`);
  }
});

server.fanout('source', {
  'job-name': [
    {queue: 'hello', name: 'world'},
    {queue: 'hello', name: 'user'}
  ]
});

client.jobEventReceiver.onComplete(event => {
  switch (event.job.name){
    case 'world':
      console.log('"world" fanout target complete, counter: ', fanoutCounter);
      break;
    case 'user':
      console.log('"user" fanout target complete, counter: ', fanoutCounter);
      break;
  }
});

client.enqueue('source', 'job-name', {username: 'Joe'}, (err) => {
  assert.equal(err, null);
  switch(fanoutCounter){
    case 0:
      console.log('only source job is guaranteed to complete by now');
      break;
    default:
      console.log('there\'s still a chance one of fanout targets is done as well');
  }
});






