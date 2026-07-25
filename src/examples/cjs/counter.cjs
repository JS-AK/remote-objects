const { Runtime } = require("../../../build/cjs/index.js");
const { Counter } = require("./actors/counter.cjs");

async function main() {
  const runtime = new Runtime({ workers: 2, debug: true });
  const counter = await runtime.spawn(Counter, 10);

  console.log("inc ->", await counter.inc());
  console.log("add(5) ->", await counter.add(5));
  console.log("getValue ->", await counter.getValue());

  await runtime.dispose();
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
