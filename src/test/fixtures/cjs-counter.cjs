const { actor } = require("../../../build/cjs/index.js");

class CjsCounter {
  constructor(value = 0) {
    this.value = value;
  }

  inc() {
    this.value += 1;
    return this.value;
  }

  getValue() {
    return this.value;
  }
}

// Default export style: module.exports = Class
actor(CjsCounter, __filename);
module.exports = CjsCounter;
