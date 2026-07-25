const { actor } = require("../../../../build/cjs/index.js");

class Counter {
  constructor(value = 0) {
    this.value = value;
  }

  inc() {
    this.value += 1;
    return this.value;
  }

  add(n) {
    this.value += n;
    return this.value;
  }

  getValue() {
    return this.value;
  }
}

actor(Counter, __filename);

module.exports = { Counter };
