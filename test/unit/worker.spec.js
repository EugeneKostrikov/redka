'use strict';

describe('worker', function(){

  it('should be bound to single queue');
  it('should be able to process many job names');
  it('should pick next job once previous is complete');
  it('should wait for new jobs when nothing can be pulled out of the queue');
  describe('plumbing', function(){
    it('should listen for new jobs on primary worker list');
    it('should move current job to "in progress" list');
    it('should move complete job to "complete" list');
    it('should move failed job to "failed" list');
  });
});