import { Counter } from "./actors/chain-counter.js";
import { Runtime } from "../index.js";

const runtime = new Runtime({ workers: 1 });

const counter = await runtime.spawn(Counter, 0);

const same = await counter.inc();

await same.inc();
await same.inc();

console.log("after chain ->", await same.getValue());
console.log("original ref ->", await counter.getValue());

await runtime.dispose();
