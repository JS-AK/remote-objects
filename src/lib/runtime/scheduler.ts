import type { ActorProxy, AnyActorClass } from "../types.js";
import type { WorkerNode } from "./worker-node.js";

export class Scheduler {
	private next = 0;

	constructor(private readonly workers: WorkerNode[]) {
		if (workers.length === 0) {
			throw new Error("Scheduler requires at least one worker");
		}
	}

	pick(): WorkerNode {
		const worker = this.workers[this.next % this.workers.length];

		this.next += 1;
		if (!worker) {
			throw new Error("No worker available");
		}

		return worker;
	}

	async create<C extends AnyActorClass>(
		Class: C,
		args: ConstructorParameters<C>,
	): Promise<ActorProxy<InstanceType<C>>> {
		const worker = this.pick();

		return worker.create(Class.name, args as unknown[]);
	}
}
