import { Readable, Writable } from "node:stream";

import { actor } from "../../index.js";

export class StreamActor {
	numbers(count: number): Readable {
		let i = 0;

		return new Readable({
			objectMode: true,
			read() {
				if (i >= count) {
					this.push(null);

					return;
				}
				this.push(i++);
			},
		});
	}

	async collect(input: Readable): Promise<unknown[]> {
		const items: unknown[] = [];

		for await (const chunk of input) {
			items.push(chunk);
		}

		return items;
	}

	sink(): { stream: Writable; done: () => Promise<unknown[]>; } {
		const items: unknown[] = [];
		let resolveDone: (value: unknown[]) => void;
		const done = new Promise<unknown[]>((resolve) => {
			resolveDone = resolve;
		});

		const stream = new Writable({
			final(cb) {
				resolveDone!(items);
				cb();
			},
			objectMode: true,
			write(chunk, _enc, cb) {
				items.push(chunk);
				cb();
			},
		});

		return { done: () => done, stream };
	}

	failingReadable(): Readable {
		return new Readable({
			objectMode: true,
			read() {
				this.destroy(new Error("stream boom"));
			},
		});
	}
}

actor(StreamActor, import.meta);
