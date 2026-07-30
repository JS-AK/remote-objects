import type { ActorProxy, AnyActorClass } from "../types.js";
import type { WorkerNode } from "./worker-node.js";

/**
 * Round-robin placement of new actors across the worker pool.
 * After spawn, an actor stays sticky on the chosen worker.
 */
export class Scheduler {
	private next = 0;

	/**
	 * @param workers - Non-empty list of {@link WorkerNode}s
	 */
	constructor(private readonly workers: WorkerNode[]) {
		if (workers.length === 0) {
			throw new Error("Scheduler requires at least one worker");
		}
	}

	/**
	 * Picks the next worker in round-robin order.
	 * @returns Worker that will host the next spawned actor
	 */
	pick(): WorkerNode {
		const worker = this.workers[this.next % this.workers.length];

		this.next += 1;
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
}
