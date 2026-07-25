import { actor } from "../../index.js";

export class Counter {
	value: number;

	constructor(value: number = 0) {
		this.value = value;
	}

	inc(): number {
		this.value += 1;

		return this.value;
	}

	/** Returns this actor — runtime turns it into a Proxy ref. */
	chain(): this {
		this.value += 1;

		return this;
	}

	add(n: number): number {
		this.value += n;

		return this.value;
	}

	getValue(): number {
		return this.value;
	}

	fail(): never {
		throw new Error("boom");
	}
}

actor(Counter, import.meta);
