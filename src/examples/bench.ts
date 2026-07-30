/* eslint-disable no-console, sort-imports */
import { Readable } from "node:stream";
import { Worker } from "node:worker_threads";

import { Runtime } from "../index.js";
import { Counter } from "./actors/counter.js";
import { Streamer } from "./actors/streamer.js";

async function time(label: string, fn: () => Promise<void>): Promise<void> {
	const start = performance.now();

	await fn();
	const ms = performance.now() - start;

	console.log(`${label}: ${ms.toFixed(1)}ms`);
}

const runtime = new Runtime({ workers: 1 });
const counter = await runtime.spawn(Counter, 0);
const streamer = await runtime.spawn(Streamer);

const CALLS = 2_000;

await time(`remote call x${CALLS}`, async () => {
	for (let i = 0; i < CALLS; i++) {
		await counter.inc();
	}
});

await time("callback round-trip x500", async () => {
	await streamer.forEachRow(500, async () => undefined);
});

await time("stream 5k rows", async () => {
	const stream = await streamer.query(5_000) as Readable;
	let n = 0;

	for await (const _row of stream) {
		n += 1;
	}
	if (n !== 5_000) throw new Error(`expected 5000, got ${n}`);
});

await time("baseline worker postMessage x2000", async () => {
	await new Promise<void>((resolve, reject) => {
		const worker = new Worker(
			`
			const { parentPort } = require("node:worker_threads");
			parentPort.on("message", (msg) => parentPort.postMessage(msg));
			`,
			{ eval: true },
		);
		let left = CALLS;

		worker.on("message", () => {
			left -= 1;
			if (left === 0) {
				void worker.terminate().then(() => resolve());
			}
		});
		worker.on("error", reject);
		for (let i = 0; i < CALLS; i++) {
			worker.postMessage(i);
		}
	});
});

await runtime.dispose();
