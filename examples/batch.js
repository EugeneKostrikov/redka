const Redka = require('../index.js');
const assert = require('assert');

let callCount = 0;
let jobsCount = 0;

const redka = new Redka();

redka.worker('target').register({
  batched: (data, cb) => {
    callCount++;
    jobsCount+= data.length;
    cb();
  }
});

redka.batch({
  sourceQueue: 'source',
  sourceName: 'distinct',
  targetQueue: 'target',
  targetName: 'batched',
  batchSize: 5,
  batchInterval: 1000
});

let firstBatchStart;
let secondBatchStart;
redka.jobEventReceiver.onComplete(event => {
  if (event.job.name === 'batched'){
    if (callCount === 1){
      assert.equal(jobsCount, 5);
      console.log('first batch complete in ', Date.now() - firstBatchStart);
      secondBatchStart = Date.now();
      redka.enqueue('source', 'distinct', {});
      redka.enqueue('source', 'distinct', {});
    }else{
      console.log('second batch done in ', Date.now() - secondBatchStart);
      assert.equal(jobsCount, 7);
      process.exit(0);
    }
  }
});

firstBatchStart = Date.now();
for (let i = 0; i < 5; i++){
  redka.enqueue('source', 'distinct', {});
}

