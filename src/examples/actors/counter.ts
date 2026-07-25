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

	add(n: number): number {
		this.value += n;

		return this.value;
	}

	getValue(): number {
		return this.value;
	}
}

actor(Counter, import.meta);
