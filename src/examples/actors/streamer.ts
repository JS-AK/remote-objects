import { Readable } from "node:stream";

import { actor } from "../../index.js";

export class Streamer {
	/** Emit `count` rows via a Node.js Readable (objectMode). */
	query(count: number): Readable {
		let i = 0;

		return new Readable({
			objectMode: true,
			read() {
				if (i >= count) {
					this.push(null);

					return;
				}
				const n = i++;

				this.push({ id: n, value: n * n });
			},
		});
	}

	async forEachRow(
		count: number,
		onRow: (row: { id: number; value: number; }) => void | Promise<void>,
	): Promise<number> {
		for (let i = 0; i < count; i++) {
			await onRow({ id: i, value: i * i });
		}

		return count;
	}
}

actor(Streamer, import.meta);
