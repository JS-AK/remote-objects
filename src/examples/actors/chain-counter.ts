import { actor } from "../../index.js";

export class Counter {
	value: number;

	constructor(value: number = 0) {
		this.value = value;
	}

	/** Returns this actor — runtime turns it into a Proxy ref. */
	inc(): this {
		this.value += 1;

		return this;
	}

	getValue(): number {
		return this.value;
	}
}

actor(Counter, import.meta);
