import { Counter } from "./actors/counter.js";
import { Runtime } from "../index.js";

const runtime = new Runtime({ debug: true, workers: 2 });

await runtime.register(Counter);

const counter = await runtime.spawn(Counter, 10);

console.log("inc ->", await counter.inc());
console.log("add(5) ->", await counter.add(5));
console.log("getValue ->", await counter.getValue());

await runtime.dispose();
