import type {
	ActorProxy,
	AnyActorClass,
	DebugHandler,
	RuntimeOptions,
} from "../types.js";

import { formatActorId } from "../protocol/refs.js";
import { getActorHandle } from "../proxy/proxy.js";
import { getActorMeta } from "../actor-meta.js";

import { Registry } from "./registry.js";
import { Scheduler } from "./scheduler.js";
import { WorkerNode } from "./worker-node.js";

function resolveDebug(debug: RuntimeOptions["debug"]): DebugHandler | undefined {
	if (!debug) return undefined;

	if (typeof debug === "function") {
		return debug;
	}

	if (debug === true) {
		return (event) => {
			console.error("[remote-objects]", event);
		};
	}

	return debug.onEvent;
}

export class Runtime {
	private readonly registry = new Registry();
	private readonly workers: WorkerNode[];
	private readonly scheduler: Scheduler;
	private readonly onDebug?: DebugHandler;
	private disposed = false;

	constructor(options: RuntimeOptions = {}) {
		const count = options.workers ?? 1;
		const onDebug = resolveDebug(options.debug);

		if (onDebug) this.onDebug = onDebug;

		const workers: WorkerNode[] = [];

		this.workers = workers;

		for (let id = 0; id < count; id++) {
			const nodeOptions: {
				id: number;
				getWorker: (workerId: number) => WorkerNode | undefined;
				onDebug?: DebugHandler;
				callTimeoutMs?: number;
			} = {
				getWorker: (workerId) => workers[workerId],
				id,
			};

			if (onDebug) nodeOptions.onDebug = onDebug;
			if (options.callTimeoutMs !== undefined) {
				nodeOptions.callTimeoutMs = options.callTimeoutMs;
			}
			workers.push(new WorkerNode(nodeOptions));
		}

		this.scheduler = new Scheduler(this.workers);
	}

	private assertOpen(action: string): void {
		if (this.disposed) {
			throw new Error(`Runtime is disposed; cannot ${action}`);
		}
	}

	/**
	 * Registers a class for workers.
	 * Bind in the actor module: `actor(Counter, import.meta)` (ESM)
	 * or `actor(Counter, __filename)` (CJS).
	 */
	async register<C extends AnyActorClass>(Class: C): Promise<void> {
		this.assertOpen("register");
		const meta = getActorMeta(Class);

		if (!meta) {
			throw new Error(
				`Class "${Class.name}" has no module binding. In the actor file add: actor(${Class.name}, import.meta) or actor(${Class.name}, __filename)`,
			);
		}

		this.registry.register(Class, meta.moduleUrl, meta.exportName);

		await Promise.all(
			this.workers.map((worker) =>
				worker.register(Class.name, meta.moduleUrl, meta.exportName),
			),
		);
	}

	async spawn<C extends AnyActorClass>(
		Class: C,
		...args: ConstructorParameters<C>
	): Promise<ActorProxy<InstanceType<C>>> {
		this.assertOpen("spawn");
		if (!this.registry.has(Class.name)) {
			await this.register(Class);
		}

		return this.scheduler.create(Class, args);
	}

	/**
	 * Removes an actor from its worker. Further method calls on the proxy fail.
	 */
	async destroy(proxy: object): Promise<void> {
		this.assertOpen("destroy");
		const handle = getActorHandle(proxy);

		if (!handle) {
			throw new Error("Value is not an actor proxy from this runtime");
		}

		const worker = this.workers[handle.workerId];

		if (!worker) {
			throw new Error(
				`Unknown actor ${formatActorId(handle.workerId, handle.objectId)}`,
			);
		}

		await worker.destroy(handle.objectId);
	}

	/**
	 * Graceful shutdown: stop new work, optionally call dispose/close on actors,
	 * wait for in-flight calls, then terminate workers.
	 */
	async dispose(options?: { closeActors?: boolean; }): Promise<void> {
		if (this.disposed) return;
		this.disposed = true;

		this.onDebug?.({
			type: "dispose",
			workers: this.workers.length,
		});

		await Promise.all(
			this.workers.map((w) =>
				w.gracefulTerminate({
					closeActors: options?.closeActors !== false,
				}),
			),
		);
	}
}
