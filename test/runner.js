'use strict';
describe('@unit', function(){
  require('./unit/fanout');
  require('./unit/worker')();
  require('./unit/redka');
  require('./unit/reporter');
  require('./unit/job');
  require('./unit/callbacks');
});

describe('@integration', function(){
  require('./integration/e2e');
});