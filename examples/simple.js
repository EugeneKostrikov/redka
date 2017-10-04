const Redka = require('../index.js');
const assert = require('assert');

const client = new Redka({});

const server = new Redka({});

const worker = server.worker('hello');
worker.register({
  world: (data, cb) => cb(null, `Hello world`),
  user: (data, cb) => cb(null, `Hello ${data.username}`),
  error: (data, cb) => cb(new Error('ERR'))
});

let counter = 0;

client.enqueue('hello', 'world', {}, (err, result) => {
  assert.equal(counter++, 0);
  assert.equal(err, null);
  assert.equal(result, 'Hello world');
  console.log(result);
});

client.enqueue('hello', 'user', {username: 'Dilbert'}, (err, result) => {
  assert.equal(counter++, 1);
  assert.equal(err, null);
  assert.equal(result, 'Hello Dilbert');
  console.log(result);
});

//Technically there's no difference between "server" and "client"
//Only that "server" has workers
server.enqueue('hello', 'error', {}, (err) => {
  assert.equal(counter++, 2);
  assert.equal(err.message, 'ERR');
  console.error(err);
  process.exit(0);
});