import { actor } from "../../index.js";

export type HasValue = {
	getValue: () => number | Promise<number>;
};

export class NestedActors {
	wrap(counter: HasValue): { counter: HasValue; label: string; } {
		return { counter, label: "wrapped" };
	}

	async readWrapped(payload: { counter: HasValue; }): Promise<number> {
		return payload.counter.getValue();
	}
}

actor(NestedActors, import.meta);
