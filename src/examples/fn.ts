import { Math } from "./actors/math.js";
import { Runtime } from "../index.js";

const runtime = new Runtime({ debug: true, workers: 2 });

const math = await runtime.spawn(Math);

console.log("add(2, 3) ->", await math.add(2, 3));
console.log("fib(10) ->", await math.fib(10));

const math2 = await runtime.spawn(Math);
const [x, y] = await Promise.all([math.fib(20), math2.fib(25)]);

console.log("parallel fib ->", { x, y });

await runtime.dispose();
