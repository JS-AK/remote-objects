import {
	describe, expect, it,
} from "vitest";

import { Scheduler } from "../lib/runtime/scheduler.js";
import type { WorkerNode } from "../lib/runtime/worker-node.js";

function mockWorker(id: number, load: number): WorkerNode {
	return { getSchedulingLoad: () => load, id } as WorkerNode;
}

describe("Scheduler", () => {
	it("picks the worker with the lowest scheduling load", () => {
		const scheduler = new Scheduler([
			mockWorker(0, 3),
			mockWorker(1, 1),
			mockWorker(2, 2),
		]);

		expect(scheduler.pick().id).toBe(1);
	});

	it("breaks load ties on the lowest worker id", () => {
		const scheduler = new Scheduler([
			mockWorker(2, 1),
			mockWorker(0, 1),
			mockWorker(1, 1),
		]);

		expect(scheduler.pick().id).toBe(0);
	});

	it("throws when the worker pool is empty", () => {
		expect(() => new Scheduler([])).toThrow(/at least one worker/);
	});

	it("pickForKey is stable for the same key", () => {
		const scheduler = new Scheduler([
			mockWorker(0, 0),
			mockWorker(1, 0),
			mockWorker(2, 0),
		]);

		expect(scheduler.pickForKey("tenant-42").id).toBe(
			scheduler.pickForKey("tenant-42").id,
		);
	});

	it("pickForKey can target different workers for different keys", () => {
		const scheduler = new Scheduler([
			mockWorker(0, 0),
			mockWorker(1, 0),
			mockWorker(2, 0),
		]);
		const ids = new Set(
			["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"].map(
				(key) => scheduler.pickForKey(key).id,
			),
		);

		expect(ids.size).toBeGreaterThan(1);
	});
});
