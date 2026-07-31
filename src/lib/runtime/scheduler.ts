import type { ActorProxy, AnyActorClass } from "../types.js";
import type { WorkerNode } from "./worker-node.js";

/** FNV-1a 32-bit hash for stable worker placement by spawn key. */
function hashKey(key: string): number {
	let h = 2_166_136_261;

	for (let i = 0; i < key.length; i++) {
		h ^= key.charCodeAt(i);
		h = Math.imul(h, 1_677_761_9);
	}

	return h >>> 0;
}

/**
 * Load-aware placement of new actors across the worker pool.
 * Picks the worker with the fewest live actors and in-flight requests;
 * ties break on the lowest worker id. After spawn, an actor stays sticky.
 */
export class Scheduler {
	/**
	 * @param workers - Non-empty list of {@link WorkerNode}s
	 */
	constructor(private readonly workers: WorkerNode[]) {
		if (workers.length === 0) {
			throw new Error("Scheduler requires at least one worker");
		}
	}

	/**
	 * Picks the least-loaded worker for the next spawn.
	 * @returns Worker that will host the next spawned actor
	 */
	pick(): WorkerNode {
		let best = this.workers[0];

		if (!best) {
			throw new Error("No worker available");
		}

		let bestLoad = best.getSchedulingLoad();

		for (let i = 1; i < this.workers.length; i++) {
			const worker = this.workers[i];

			if (!worker) continue;
			const load = worker.getSchedulingLoad();

			if (load < bestLoad || (load === bestLoad && worker.id < best.id)) {
				best = worker;
				bestLoad = load;
			}
		}

		return best;
	}

	/**
	 * Picks a worker deterministically from a spawn key (stable across the pool size).
	 * @param key - Non-empty affinity key (tenant id, shard name, etc.)
	 * @returns Worker that will host actors for this key
	 */
	pickForKey(key: string): WorkerNode {
		const index = hashKey(key) % this.workers.length;
		const worker = this.workers[index];

		if (!worker) {
			throw new Error("No worker available");
		}

		return worker;
	}

	/**
	 * Spawns an actor on the next worker and returns a typed proxy.
	 * @param Class - Actor class (already registered on workers)
	 * @param args - Constructor arguments
	 * @returns Typed proxy for the new actor
	 */
	async create<C extends AnyActorClass>(
		Class: C,
		args: ConstructorParameters<C>,
	): Promise<ActorProxy<InstanceType<C>>> {
		const worker = this.pick();

		return worker.create(Class.name, args as unknown[]);
	}

	/**
	 * Spawns an actor on a specific worker and returns a typed proxy.
	 * @param worker - Target {@link WorkerNode}
	 * @param Class - Actor class (already registered on workers)
	 * @param args - Constructor arguments
	 * @returns Typed proxy for the new actor
	 */
	async createOn<C extends AnyActorClass>(
		worker: WorkerNode,
		Class: C,
		args: ConstructorParameters<C>,
	): Promise<ActorProxy<InstanceType<C>>> {
		return worker.create(Class.name, args as unknown[]);
	}
}
