# Redka - Minimalistic queue for node.js apps
## Features
* Powered by Redis
* Optionally log results into Mongo for easy inspection with included plugin
* Delayed jobs
* Job completion callbacks - client-side can optionally wait for the job to complete 
* Sequential by default, optionally parallel
* Job context allowing fine-grained control over execution
* Job status events on Redis pub-sub

##Usage
```javascript
//server.js
const Redka = require('redka');
const server = new Redka({});
const worker = server.worker('hello');
worker.register({
  world: (params, cb) => cb(null, `Hello world, I'm ${params.username}`)
});
```
```javascript
//client.js
const Redka = require('redka');

const client = new Redka({});
const assert = require('assert');
client.enqueue('hello', 'world', {username: 'Joe'}, (err, result) => {
  assert.equal(err, null);
  assert.equal(result, 'Hello world, I\'m Joe');
});
```

##API reference
### Redka(configuration) 
Creates new redka instance. Created instance can act as both "client" and "server". There's in fact no such separation, 
it's only for ease of communication. Instance is considered to be "server" if it has workers attached. It still can use all 
"client" methods though.
```
configuration - Object, required. {
  redis: { Anything acceptable for redis npm package npm package npm package
    host: 'localhost',
    port: 6570
  },
  prefix: 'redka_', Optional prefix for queue names
  runDelayedJobsManager: false, Only one instance of redka in your cluster should have this enabled. Nothing harmful if more, just suboptimal
  delayOptions: {
    pollInterval: 100, Minimal delay resolution
  }
}
```
#### .worker(queueName, options) Worker
Creates and returns new worker instance
```
queueName - String, required. Queue name for worker to pull jobs from
options - Object, optional. {
  parallel: 1, Number. Max number of jobs to be processed in parallel from the queue
  timeout: null, Number(seconds) Fail job taking more than X seconds to run
}
```
#### .enqueue(queueName, jobName, params, options, callback) null
Creates and puts new job into provided queue
```
queueName - String, required. Target queue for created job
jobName - String, required. Job name
params - Object, required. Job parameters. Must be serialisable to JSON
options - Object, optional. Job options {
  delay: null, Date or Number(seconds). When to start processing job. Requires running delayedJobManager instance
}
callback - Function, optional. Called when enqueued job is complete
```
#### .stop(callback) null
Stops all workers attached to current Redka instance. Waits for dequeued jobs to complete
```
callback - Function, required. Called when all workers are done processing already dequeued jobs.
```
#### .fanout(sourceQueue, configuration) null
Creates default "fanout" worker. The worker would pick up all jobs from provided source queue
and push into configured targets. See examples/fanout.js for working example.
```
sourceQueue - String, required.
configuration - Object, required {
  job-name-to-clone-into-targets: [
    //array of destinations
    {queue: 'target-queue-one', name: 'target-job-name-one'},
    {queue: 'target-queue-one', name: 'target-job-name-two'},
    ...
  ]
}
```
#### .batch(configuration) null
Creates default batch worker. The worker would accumulate jobs from source queue into batches
and create target job when either batchInterval timeout fires or batch length equals to
batchSize option. See examples/batch.js for more.
```
configuration - Object, required {
  sourceQueue - String, required
  sourceName - String, required
  targetQueue - String, required
  targetName - String, required
  batchSize - Number, required
  batchInterval - Number, options, defaults to 1000ms
}
```

### Worker
#### .register(callbacks) null
Attaches handlers to the worker and starts it if not running yet. Can be extended or overwritten when it's already running.
But keep in mind - jobs with no handler will fail as soon as worker starts.
```
callbacks - Object, required. A map of {job-name: handlerFunction} pairs
```

## License
MIT
