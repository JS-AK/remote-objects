import { actor } from "../../index.js";

export type Readable = {
	getValue: () => number | Promise<number>;
};

export class Linker {
	private other: Readable | null = null;

	link(other: Readable): void {
		this.other = other;
	}

	async readOther(): Promise<number> {
		if (!this.other) {
			throw new Error("not linked");
		}

		return this.other.getValue();
	}
}

actor(Linker, import.meta);
