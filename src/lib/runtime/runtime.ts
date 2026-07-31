/* eslint-disable sort-imports */
import type {
	ActorProxy,
	AnyActorClass,
	DebugHandler,
	DestroyOptions,
	RuntimeOptions,
} from "../types.js";

import { formatActorId } from "../protocol/refs.js";
import { getActorHandle } from "../proxy/proxy.js";
import { getActorMeta } from "../actor-meta.js";
import { CallbackRegistry } from "../protocol/callback-registry.js";
import { isPlainObject } from "../protocol/plain.js";
import { StreamBridge } from "../protocol/stream-bridge.js";

import { createHostStreamTransport } from "./host-stream-transport.js";
import { Registry } from "./registry.js";
import { Scheduler } from "./scheduler.js";
import { StreamRouter } from "./stream-router.js";
import { WorkerNode } from "./worker-node.js";

/** Host-side singleton entry for {@link Runtime.getOrSpawn}. */
type KeyedEntry = {
	proxy: object;
	className: string;
	args: unknown[];
};

/**
 * Deep equality for structured-clone constructor args (primitives, arrays, plain objects).
 * @param a - First value
 * @param b - Second value
 * @returns Whether both values are deeply equal
 */
function valueEqual(a: unknown, b: unknown): boolean {
	if (a === b) return true;
	if (a === null || b === null) return false;
	if (typeof a !== typeof b) return false;

	if (Array.isArray(a) && Array.isArray(b)) {
		if (a.length !== b.length) return false;

		return a.every((item, index) => valueEqual(item, b[index]));
	}

	if (isPlainObject(a) && isPlainObject(b)) {
		const keysA = Object.keys(a);
		const keysB = Object.keys(b);

		if (keysA.length !== keysB.length) return false;

		return keysA.every((key) => valueEqual(a[key], b[key]));
	}

	return false;
}

/**
 * @param a - Args from an existing keyed entry
 * @param b - Args from a new {@link Runtime.getOrSpawn} call
 * @returns Whether constructor argument lists match
 */
function argsEqual(a: unknown[], b: unknown[]): boolean {
	if (a.length !== b.length) return false;

	return a.every((item, index) => valueEqual(item, b[index]));
}

/**
 * Normalizes {@link RuntimeOptions.debug} into a single event handler.
 * @param debug - Boolean, function, or `{ onEvent }` form
 * @returns Handler to invoke, or `undefined` when debug is off
 */
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

/**
 * Owns a pool of worker threads, schedules sticky actors, and exposes
 * typed proxies for remote method calls (including callbacks and streams).
 * Use {@link spawn} for new actors (load-balanced) or {@link getOrSpawn} for
 * one actor per key until {@link destroy}.
 */
export class Runtime {
	private readonly registry = new Registry();
	private readonly workers: WorkerNode[];
	private readonly scheduler: Scheduler;
	private readonly onDebug?: DebugHandler;
	private readonly hostCallbacks = new CallbackRegistry("host");
	private readonly streamRouter = new StreamRouter();
	private readonly hostStreams: StreamBridge;
	private disposed = false;
	/** One actor proxy per spawn key ({@link Runtime.getOrSpawn}). */
	private readonly keyed = new Map<string, KeyedEntry>();

	/**
	 * @param options - Pool size, debug hooks, and optional call timeout
	 */
	constructor(options: RuntimeOptions = {}) {
		const count = options.workers ?? 1;
		const onDebug = resolveDebug(options.debug);

		if (onDebug) this.onDebug = onDebug;

		const workers: WorkerNode[] = [];

		this.workers = workers;
		this.hostStreams = new StreamBridge("host", (ref) =>
			createHostStreamTransport(() => workers, this.streamRouter, ref),
		);

		for (let id = 0; id < count; id++) {
			const nodeOptions: ConstructorParameters<typeof WorkerNode>[0] = {
				getWorker: (workerId) => workers[workerId],
				hostCallbacks: this.hostCallbacks,
				hostStreams: this.hostStreams,
				id,
				streamRouter: this.streamRouter,
			};

			if (onDebug) nodeOptions.onDebug = onDebug;
			if (options.callTimeoutMs !== undefined) {
				nodeOptions.callTimeoutMs = options.callTimeoutMs;
			}
			workers.push(new WorkerNode(nodeOptions));
		}

		this.scheduler = new Scheduler(this.workers);
	}

	/**
	 * Throws if the runtime has already been disposed.
	 * @param action - Human-readable action name for the error message
	 */
	private assertOpen(action: string): void {
		if (this.disposed) {
			throw new Error(`Runtime is disposed; cannot ${action}`);
		}
	}

	/**
	 * Registers a class for workers.
	 * Bind in the actor module: `actor(Counter, import.meta)` (ESM)
	 * or `actor(Counter, __filename)` (CJS).
	 *
	 * @param Class - Actor class previously bound with {@link actor}
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

	/**
	 * Spawns an actor on the least-loaded worker and returns a typed proxy.
	 * Auto-registers the class on first use if needed.
	 *
	 * @param Class - Actor class
	 * @param args - Constructor arguments (structured-clone + refs)
	 * @returns Typed proxy; all methods are async from the caller side
	 */
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
	 * Returns an existing actor for `key`, or spawns one on the worker chosen by
	 * `hash(key) % workers`. The same key always maps to the same proxy until
	 * {@link destroy} removes it. Reusing a key with a different class or
	 * constructor args throws.
	 *
	 * Auto-registers the class on first use if needed.
	 *
	 * @param key - Non-empty affinity key (tenant id, shard name, etc.)
	 * @param Class - Actor class
	 * @param args - Constructor arguments (must match on later calls for this key)
	 * @returns Typed proxy; all methods are async from the caller side
	 * @throws If `key` is empty, or the key is already bound to another class or args
	 */
	async getOrSpawn<C extends AnyActorClass>(
		key: string,
		Class: C,
		...args: ConstructorParameters<C>
	): Promise<ActorProxy<InstanceType<C>>> {
		this.assertOpen("getOrSpawn");

		if (key.length === 0) {
			throw new Error("getOrSpawn key must be a non-empty string");
		}

		const existing = this.keyed.get(key);

		if (existing) {
			if (existing.className !== Class.name) {
				throw new Error(
					`getOrSpawn key "${key}" is already bound to ${existing.className}, not ${Class.name}`,
				);
			}

			if (!argsEqual(existing.args, args as unknown[])) {
				throw new Error(
					`getOrSpawn key "${key}" was created with different constructor arguments`,
				);
			}

			return existing.proxy as ActorProxy<InstanceType<C>>;
		}

		if (!this.registry.has(Class.name)) {
			await this.register(Class);
		}

		const worker = this.scheduler.pickForKey(key);
		const proxy = await this.scheduler.createOn(
			worker,
			Class,
			args,
		);

		this.keyed.set(key, {
			args: args as unknown[],
			className: Class.name,
			proxy,
		});

		return proxy;
	}

	/**
	 * Removes an actor from its worker. Further method calls on the proxy fail.
	 * By default calls `dispose`/`close` on the actor first.
	 * Also drops a {@link getOrSpawn} entry when the destroyed proxy matches.
	 *
	 * @param proxy - Proxy returned by {@link spawn} or {@link getOrSpawn}
	 * @param options - Pass `{ close: false }` to skip actor cleanup
	 */
	async destroy(proxy: object, options?: DestroyOptions): Promise<void> {
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

		await worker.destroy(handle.objectId, options);
		this.releaseKeyedProxy(proxy);
	}

	/**
	 * Drops a {@link getOrSpawn} entry when its proxy is destroyed.
	 * @param proxy - Actor proxy being removed
	 */
	private releaseKeyedProxy(proxy: object): void {
		for (const [key, entry] of this.keyed) {
			if (entry.proxy === proxy) {
				this.keyed.delete(key);

				return;
			}
		}
	}

	/**
	 * Graceful shutdown: stop new work, optionally call dispose/close on actors,
	 * wait for in-flight calls, then terminate workers.
	 *
	 * @param options - Pass `{ closeActors: false }` to skip per-actor close
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

		this.hostStreams.closeAll();
		this.hostCallbacks.clear();
		this.streamRouter.clear();
		this.keyed.clear();
	}
}
