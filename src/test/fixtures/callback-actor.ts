import { actor } from "../../index.js";

export class CallbackActor {
	async withProgress(
		n: number,
		onProgress: (value: number) => void | Promise<void>,
	): Promise<number> {
		let sum = 0;

		for (let i = 1; i <= n; i++) {
			sum += i;
			await onProgress(i);
		}

		return sum;
	}

	async callMaybe(
		fn: () => number | Promise<number>,
	): Promise<number> {
		return fn();
	}

	makeAdder(base: number): (x: number) => number {
		return (x) => base + x;
	}

	async boom(fn: () => void | Promise<void>): Promise<void> {
		await fn();
	}
}

actor(CallbackActor, import.meta);
