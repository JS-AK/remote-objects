import { add, fib } from "./math-fns.js";
import { actor } from "../../index.js";

/**
 * Actor class that calls plain functions from another module.
 * Worker loads this file via `actor(..., import.meta)` and resolves
 * `./math-fns.js` relative to it — no extra registration needed.
 */
export class Math {
	add(a: number, b: number): number {
		return add(a, b);
	}

	fib(n: number): number {
		return fib(n);
	}
}

actor(Math, import.meta);
