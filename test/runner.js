'use strict';
describe('@unit', function(){
  require('./unit/fanout');
  require('./unit/worker')();
  require('./unit/delayed-jobs-manager');
  require('./unit/redka');
  require('./unit/reporter');
  require('./unit/job');
  require('./unit/job-context');
  require('./unit/callbacks');
});

describe('@integration', function(){
  require('./integration/e2e');
});