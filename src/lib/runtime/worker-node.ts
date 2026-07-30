/* eslint-disable sort-imports */
import { Worker } from "node:worker_threads";
import path from "node:path";

import type {
	ActorHandle, DebugHandler, DestroyOptions,
} from "../types.js";
import type {
	ActorRef,
	BridgeCallMessage,
	BridgeCallbackInvokeMessage,
	BridgeCallbackResultMessage,
	BridgeResultMessage,
	BridgeStreamMessage,
	CallbackRef,
	ProtocolMessage,
	ProtocolResponse,
	StreamRef,
} from "../protocol/messages.js";
import { fromWireError, toWireError } from "../protocol/messages.js";
import { formatActorId, isStreamRef } from "../protocol/refs.js";
import { createProxy, getActorHandle } from "../proxy/proxy.js";
import { Serializer } from "../protocol/serializer.js";
import { CallbackRegistry } from "../protocol/callback-registry.js";
import { StreamBridge } from "../protocol/stream-bridge.js";
import { cloneErrorMessage } from "../protocol/plain.js";
import { getModuleDir } from "./module-dir.js";
import type { StreamRouter } from "./stream-router.js";

/** Pending host↔worker request waiting for a {@link ProtocolResponse}. */
type Pending = {
	reject: (reason?: unknown) => void;
	resolve: (value: unknown) => void;
};

/** Construction options for {@link WorkerNode}. */
export type WorkerNodeOptions = {
	/** Worker index in the runtime pool. */
	id: number;
	onDebug?: DebugHandler;
	callTimeoutMs?: number;
	/** Lookup other workers for bridge routing. */
	getWorker: (workerId: number) => WorkerNode | undefined;
	/** Shared host-side callback registry. */
	hostCallbacks: CallbackRegistry;
	/** Shared host-side stream bridge. */
	hostStreams: StreamBridge;
	/** Shared stream consumer subscription table. */
	streamRouter: StreamRouter;
};

// Always load the ESM worker: CJS emit rewrites `import()` to `require()`,
// which cannot load actor modules via file: URLs.
// getModuleDir() → build/{esm|cjs}/lib/runtime
const packageRoot = path.resolve(getModuleDir(), "../../../..");
const workerEntry = path.join(
	packageRoot,
	"build",
	"esm",
	"lib",
	"worker",
	"runtime-worker.js",
);

/**
 * Host-side handle for one worker thread.
 * Owns the mailbox, pending requests, encode/decode, and bridge routing
 * for actors sticky to this worker.
 */
export class WorkerNode {
	readonly id: number;
	private readonly worker: Worker;
	private readonly pending = new Map<number, Pending>();
	private readonly serializer = new Serializer();
	private readonly onDebug?: DebugHandler;
	private readonly callTimeoutMs?: number;
	private readonly getWorker: (workerId: number) => WorkerNode | undefined;
	private readonly hostCallbacks: CallbackRegistry;
	private readonly hostStreams: StreamBridge;
	private readonly streamRouter: StreamRouter;
	private nextRequestId = 1;
	private nextObjectId = 1;
	private closed = false;
	private readonly localObjects = new Set<number>();
	/** Per-actor mailbox: serialize calls to the same objectId. */
	private readonly mailboxes = new Map<number, Promise<unknown>>();
	private drainWaiters: Array<() => void> = [];

	/**
	 * Spawns the ESM worker entry and wires message / error handlers.
	 * @param options - Pool identity, shared registries, timeouts
	 */
	constructor(options: WorkerNodeOptions) {
		this.id = options.id;
		this.getWorker = options.getWorker;
		this.hostCallbacks = options.hostCallbacks;
		this.hostStreams = options.hostStreams;
		this.streamRouter = options.streamRouter;
		if (options.onDebug) this.onDebug = options.onDebug;
		if (options.callTimeoutMs !== undefined) {
			this.callTimeoutMs = options.callTimeoutMs;
		}

		this.worker = new Worker(workerEntry, {
			workerData: { workerId: this.id },
		});

		this.worker.on("message", (msg: unknown) => {
			void this.onWorkerMessage(msg);
		});

		this.worker.on("error", (err: Error) => {
			this.onDebug?.({
				error: err.message,
				type: "worker:error",
				workerId: this.id,
			});
			for (const [, p] of this.pending) {
				p.reject(err);
			}
			this.pending.clear();
			this.notifyDrain();
		});
	}

	/**
	 * Dispatches inbound worker messages: protocol responses and bridge traffic.
	 * @param msg - Raw `worker_threads` message
	 */
	private async onWorkerMessage(msg: unknown): Promise<void> {
		if (typeof msg !== "object" || msg === null) return;

		if ("bridge" in msg) {
			const bridge = (msg as { bridge: string; }).bridge;

			if (bridge === "call") {
				void this.handleBridgeCall(msg as BridgeCallMessage);

				return;
			}

			if (bridge === "callback_invoke") {
				void this.handleBridgeCallbackInvoke(
					msg as BridgeCallbackInvokeMessage,
				);

				return;
			}

			if (bridge === "callback_result") {
				this.handleBridgeCallbackResult(msg as BridgeCallbackResultMessage);

				return;
			}

			if (typeof bridge === "string" && bridge.startsWith("stream_")) {
				void this.handleBridgeStream(msg as BridgeStreamMessage);

				return;
			}

			return;
		}

		const response = msg as ProtocolResponse;
		const pending = this.pending.get(response.id);

		if (!pending) return;
		this.pending.delete(response.id);
		this.notifyDrain();

		if ("error" in response) {
			pending.reject(fromWireError(response.error));

			return;
		}

		try {
			pending.resolve(this.decodeValue(response.result));
		} catch (err) {
			pending.reject(err);
		}
	}

	/**
	 * Resolves an {@link ActorRef} to a host-side proxy on the owning worker.
	 * @param ref - Wire actor tag from decode
	 * @returns Host-side proxy for the actor on its owning worker
	 */
	private resolveActorRef(ref: ActorRef): unknown {
		const target = this.getWorker(ref.workerId);

		if (!target) {
			throw new Error(
				`Unknown worker ${ref.workerId} for actor ${formatActorId(ref.workerId, ref.objectId)}`,
			);
		}

		return target.createLocalProxy(ref.objectId);
	}

	/**
	 * Builds an async stub that invokes a callback on its owner side.
	 * @param ref - Wire callback tag from decode
	 * @returns Async function that invokes the callback on its owner side
	 */
	private resolveCallbackRef(ref: CallbackRef): unknown {
		if (ref.owner === "host") {
			return (...args: unknown[]) =>
				this.hostCallbacks.invoke(ref.callbackId, args);
		}

		if (ref.owner === this.id) {
			return (...args: unknown[]) =>
				this.invokeWorkerCallback(ref.callbackId, args);
		}

		const target = this.getWorker(ref.owner);

		if (!target) {
			throw new Error(
				`Unknown worker ${ref.owner} for callback ${ref.callbackId}`,
			);
		}

		return (...args: unknown[]) =>
			target.invokeWorkerCallback(ref.callbackId, args);
	}

	/**
	 * Subscribes the host as consumer and creates a local stream proxy.
	 * @param ref - Wire stream tag from decode
	 * @returns Local stream proxy for the remote stream
	 */
	private resolveStreamRef(ref: StreamRef): unknown {
		this.streamRouter.subscribe(ref.owner, ref.streamId, "host");

		return this.hostStreams.createProxy(ref);
	}

	/**
	 * Decodes a wire value using this node's resolvers.
	 * @param value - Encoded wire value
	 * @returns Decoded local value (proxies, callbacks, streams)
	 */
	private decodeValue(value: unknown): unknown {
		return this.serializer.decode(value, {
			resolveActorRef: (ref) => this.resolveActorRef(ref),
			resolveCallbackRef: (ref) => this.resolveCallbackRef(ref),
			resolveStreamRef: (ref) => this.resolveStreamRef(ref),
		});
	}

	/**
	 * Encodes a host-side value for the wire (actors, callbacks, streams).
	 * @param value - Args tree or return value
	 * @param options - Optional call-scoped callback tracking / bound actor id
	 * @returns Encoded wire value
	 */
	private encodeValue(
		value: unknown,
		options?: {
			callScopedCallbacks?: number[];
			boundObjectId?: number;
		},
	): unknown {
		return this.serializer.encode(value, {
			registerCallback: (fn) => {
				const ref = this.hostCallbacks.register(
					fn as (...args: unknown[]) => unknown,
					{
						callScoped: options?.callScopedCallbacks !== undefined,
						...(options?.boundObjectId !== undefined
							? { boundObjectId: options.boundObjectId }
							: {}),
					},
				);

				options?.callScopedCallbacks?.push(ref.callbackId);

				return ref;
			},
			registerStream: (obj) => {
				const ref = this.hostStreams.tryRegisterLocal(obj);

				if (ref) {
					this.streamRouter.subscribe(ref.owner, ref.streamId, this.id);
				}

				return ref;
			},
			resolveProxy: (v) => getActorHandle(v),
			workerId: this.id,
		});
	}

	/**
	 * Builds a typed proxy for an actor sticky to this worker.
	 * @param objectId - Local object id on this worker
	 * @returns Typed host-side proxy for the actor
	 */
	createLocalProxy<T extends object>(objectId: number): T {
		const handle: ActorHandle = { objectId, workerId: this.id };

		return createProxy(handle, (id, method, methodArgs) =>
			this.call(id, method, methodArgs),
		) as T;
	}

	/**
	 * Invokes a callback registered inside this worker.
	 * @param callbackId - Id in the worker's callback registry
	 * @param args - Already-local (or to-be-encoded) arguments
	 * @returns Decoded callback result
	 */
	async invokeWorkerCallback(
		callbackId: number,
		args: unknown[],
	): Promise<unknown> {
		const encodedArgs = this.encodeValue(args) as unknown[];

		return this.request({
			args: encodedArgs,
			callbackId,
			command: "callback_invoke",
			id: this.nextRequestId++,
		});
	}

	/**
	 * Handles a worker-posted callback invoke (host or cross-worker owner).
	 * @param msg - Bridge callback invoke from this worker
	 */
	private async handleBridgeCallbackInvoke(
		msg: BridgeCallbackInvokeMessage,
	): Promise<void> {
		try {
			const decodedArgs = this.decodeValue(msg.args) as unknown[];
			let result: unknown;

			if (msg.owner === "host") {
				result = await this.hostCallbacks.invoke(msg.callbackId, decodedArgs);
			} else {
				const target = this.getWorker(msg.owner);

				if (!target) {
					throw new Error(`Unknown worker ${msg.owner}`);
				}
				result = await target.invokeWorkerCallback(
					msg.callbackId,
					decodedArgs,
				);
			}

			const reply: BridgeCallbackResultMessage = {
				bridge: "callback_result",
				id: msg.id,
				result: this.encodeValue(result),
			};

			this.post(reply);
		} catch (err) {
			const reply: BridgeCallbackResultMessage = {
				bridge: "callback_result",
				error: toWireError(err),
				id: msg.id,
			};

			this.post(reply);
		}
	}

	/**
	 * Resolves a host-pending bridge callback result (rarely used; worker holds bridgePending).
	 * @param msg - Callback result message
	 */
	private handleBridgeCallbackResult(msg: BridgeCallbackResultMessage): void {
		const pending = this.pending.get(msg.id);

		if (!pending) return;
		this.pending.delete(msg.id);
		this.notifyDrain();

		if ("error" in msg) {
			pending.reject(fromWireError(msg.error));

			return;
		}

		pending.resolve(this.decodeValue(msg.result));
	}

	/**
	 * Routes stream bridge messages between host streams and worker peers.
	 * @param msg - Stream control/data message from this worker
	 */
	private async handleBridgeStream(msg: BridgeStreamMessage): Promise<void> {
		const { owner, streamId } = msg;

		if (msg.bridge === "stream_data" && msg.direction === "from_owner") {
			for (const consumer of this.streamRouter.consumers(owner, streamId)) {
				if (consumer === "host") {
					this.hostStreams.onRemoteData(owner, streamId, msg.chunk);
				} else if (consumer !== this.id) {
					this.getWorker(consumer)?.postToWorker(msg);
				} else {
					this.postToWorker(msg);
				}
			}

			return;
		}

		if (msg.bridge === "stream_end" && msg.direction === "from_owner") {
			for (const consumer of this.streamRouter.consumers(owner, streamId)) {
				if (consumer === "host") {
					this.hostStreams.onRemoteEnd(owner, streamId);
				} else if (consumer !== this.id) {
					this.getWorker(consumer)?.postToWorker(msg);
				} else {
					this.postToWorker(msg);
				}
			}

			return;
		}

		if (msg.bridge === "stream_error") {
			const err = fromWireError(msg.error);

			if (msg.direction === "from_owner") {
				for (const consumer of this.streamRouter.consumers(owner, streamId)) {
					if (consumer === "host") {
						this.hostStreams.onRemoteError(owner, streamId, err);
					} else if (consumer !== this.id) {
						this.getWorker(consumer)?.postToWorker(msg);
					} else {
						this.postToWorker(msg);
					}
				}
			} else if (owner === "host") {
				this.hostStreams.onRemoteError(owner, streamId, err);
			} else {
				this.getWorker(owner)?.postToWorker(msg);
			}

			return;
		}

		if (msg.bridge === "stream_pause") {
			if (owner === "host") this.hostStreams.onPause(owner, streamId);
			else if (owner === this.id) this.postToWorker(msg);
			else this.getWorker(owner)?.postToWorker(msg);

			return;
		}

		if (msg.bridge === "stream_resume") {
			if (owner === "host") this.hostStreams.onResume(owner, streamId);
			else if (owner === this.id) this.postToWorker(msg);
			else this.getWorker(owner)?.postToWorker(msg);

			return;
		}

		if (msg.bridge === "stream_write") {
			if (owner === "host") {
				await this.hostStreams.onWrite(owner, streamId, msg.id, msg.chunk);
			} else if (owner === this.id) {
				this.postToWorker(msg);
			} else {
				this.getWorker(owner)?.postToWorker(msg);
			}

			return;
		}

		if (msg.bridge === "stream_write_end") {
			if (owner === "host") {
				await this.hostStreams.onWriteEnd(owner, streamId, msg.id);
			} else if (owner === this.id) {
				this.postToWorker(msg);
			} else {
				this.getWorker(owner)?.postToWorker(msg);
			}

			return;
		}

		if (msg.bridge === "stream_write_result") {
			if (this.hostStreams.has(owner, streamId)) {
				this.hostStreams.onWriteResult(
					owner,
					streamId,
					msg.id,
					msg.error ? fromWireError(msg.error) : undefined,
				);
			} else {
				for (const consumer of this.streamRouter.consumers(owner, streamId)) {
					if (consumer !== "host") {
						this.getWorker(consumer)?.postToWorker(msg);
					}
				}
			}

			return;
		}

		if (msg.bridge === "stream_close") {
			if (this.hostStreams.has(owner, streamId)) {
				this.hostStreams.onClose(owner, streamId);
			}
			this.streamRouter.unsubscribeAll(owner, streamId);
			if (owner === this.id) this.postToWorker(msg);
			else if (owner !== "host") this.getWorker(owner)?.postToWorker(msg);
		}
	}

	/**
	 * Posts an arbitrary message into this worker (bridge fan-out helper).
	 * @param msg - Message payload
	 */
	postToWorker(msg: unknown): void {
		this.post(msg);
	}

	/**
	 * Forwards a cross-worker actor call to the target {@link WorkerNode}.
	 * @param msg - Bridge call from this worker's stub
	 */
	private async handleBridgeCall(msg: BridgeCallMessage): Promise<void> {
		this.onDebug?.({
			method: msg.method,
			objectId: msg.objectId,
			requestId: msg.id,
			targetWorkerId: msg.targetWorkerId,
			type: "bridge:call",
			workerId: this.id,
		});

		try {
			const target = this.getWorker(msg.targetWorkerId);

			if (!target) {
				throw new Error(`Unknown worker ${msg.targetWorkerId}`);
			}

			const decodedArgs = this.decodeValue(msg.args) as unknown[];
			const result = await target.call(msg.objectId, msg.method, decodedArgs);
			const encoded = target.encodeForBridge(result);

			this.subscribeStreamsIn(encoded, this.id);

			const reply: BridgeResultMessage = {
				bridge: "result",
				id: msg.id,
				result: encoded,
			};

			this.post(reply);
			this.onDebug?.({
				requestId: msg.id,
				targetWorkerId: msg.targetWorkerId,
				type: "bridge:result",
				workerId: this.id,
			});
		} catch (err) {
			const reply: BridgeResultMessage = {
				bridge: "result",
				error: toWireError(err),
				id: msg.id,
			};

			this.post(reply);
			this.onDebug?.({
				error: err instanceof Error ? err.message : String(err),
				requestId: msg.id,
				targetWorkerId: msg.targetWorkerId,
				type: "bridge:result",
				workerId: this.id,
			});
		}
	}

	/**
	 * Encode a host-side value for a bridge reply (may include stream/callback refs).
	 * @param value - Call result to send back to the requesting worker
	 * @returns Encoded value for the bridge reply
	 */
	encodeForBridge(value: unknown): unknown {
		return this.encodeValue(value);
	}

	/**
	 * Walks an encoded tree and subscribes `consumer` to any nested stream refs.
	 * @param value - Encoded (or partially decoded) tree
	 * @param consumer - Side that will hold proxies for those streams
	 */
	private subscribeStreamsIn(
		value: unknown,
		consumer: "host" | number,
	): void {
		if (isStreamRef(value)) {
			this.streamRouter.subscribe(value.owner, value.streamId, consumer);

			return;
		}
		if (!value || typeof value !== "object") return;
		if (Array.isArray(value)) {
			for (const item of value) this.subscribeStreamsIn(item, consumer);

			return;
		}
		for (const item of Object.values(value)) {
			this.subscribeStreamsIn(item, consumer);
		}
	}

	/**
	 * Throws if this worker node has been closed by dispose.
	 * @param action - Human-readable action for the error message
	 */
	assertOpen(action: string): void {
		if (this.closed) {
			throw new Error(`Runtime is disposed; cannot ${action}`);
		}
	}

	/**
	 * Tells the worker to load and register an actor class module.
	 * @param className - Registry key / class name
	 * @param moduleUrl - Absolute URL for worker `import()`
	 * @param exportName - Named export inside the module
	 */
	async register(
		className: string,
		moduleUrl: string,
		exportName: string,
	): Promise<void> {
		this.assertOpen("register");
		await this.request({
			className,
			command: "register",
			exportName,
			id: this.nextRequestId++,
			moduleUrl,
		});

		this.onDebug?.({
			className,
			type: "register",
			workerId: this.id,
		});
	}

	/**
	 * Creates an actor instance on this worker and returns a host proxy.
	 * @param className - Registered class name
	 * @param args - Constructor arguments (encoded before send)
	 * @returns Host-side proxy for the new actor
	 */
	async create<T extends object>(
		className: string,
		args: unknown[],
	): Promise<T> {
		this.assertOpen("spawn");
		const objectId = this.nextObjectId++;
		const callScoped: number[] = [];

		this.localObjects.add(objectId);

		try {
			await this.request({
				args: this.encodeValue(args, {
					callScopedCallbacks: callScoped,
				}) as unknown[],
				className,
				command: "create",
				id: this.nextRequestId++,
				objectId,
			});
		} finally {
			this.hostCallbacks.releaseCallScoped(callScoped);
		}

		this.onDebug?.({
			actorId: formatActorId(this.id, objectId),
			className,
			objectId,
			type: "spawn",
			workerId: this.id,
		});

		return this.createLocalProxy(objectId);
	}

	/**
	 * Destroys an actor on this worker (optionally calling dispose/close first).
	 * @param objectId - Local actor id
	 * @param options - `{ close: false }` skips actor cleanup
	 */
	async destroy(
		objectId: number,
		options?: DestroyOptions,
	): Promise<void> {
		this.assertOpen("destroy");
		if (!this.localObjects.has(objectId)) {
			throw new Error(
				`Unknown actor ${formatActorId(this.id, objectId)} on worker ${this.id}`,
			);
		}

		const close = options?.close !== false;

		await this.enqueue(objectId, async () => {
			await this.request({
				close,
				command: "destroy",
				id: this.nextRequestId++,
				objectId,
			});
		});

		this.localObjects.delete(objectId);
		this.mailboxes.delete(objectId);
		this.hostCallbacks.releaseBoundToObject(objectId);

		this.onDebug?.({
			actorId: formatActorId(this.id, objectId),
			objectId,
			type: "destroy",
			workerId: this.id,
		});
	}

	/**
	 * Enqueues a method call on an actor's mailbox.
	 * @param objectId - Local actor id
	 * @param method - Method name
	 * @param args - Host-side arguments (encoded before send)
	 * @returns Decoded method result
	 */
	async call(
		objectId: number,
		method: string,
		args: unknown[],
	): Promise<unknown> {
		this.assertOpen(`call ${method}`);

		return this.enqueue(objectId, () => this.doCall(objectId, method, args));
	}

	/**
	 * Chains a task onto the per-actor mailbox promise.
	 * @param objectId - Actor whose mailbox to use
	 * @param task - Async work to run serially for this actor
	 * @returns Result of the enqueued task
	 */
	private enqueue<T>(objectId: number, task: () => Promise<T>): Promise<T> {
		const prev = this.mailboxes.get(objectId) ?? Promise.resolve();
		const next = prev.then(task, task);

		this.mailboxes.set(
			objectId,
			next.then(
				() => undefined,
				() => undefined,
			),
		);

		return next;
	}

	/**
	 * Performs one remote call with debug events, timeout, and call-scoped callbacks.
	 * @param objectId - Local actor id
	 * @param method - Method name
	 * @param args - Host-side arguments
	 * @returns Decoded call result
	 */
	private async doCall(
		objectId: number,
		method: string,
		args: unknown[],
	): Promise<unknown> {
		const requestId = this.nextRequestId++;
		const started = performance.now();
		const actorId = formatActorId(this.id, objectId);
		const callScoped: number[] = [];

		this.onDebug?.({
			actorId,
			method,
			objectId,
			requestId,
			type: "call:start",
			workerId: this.id,
		});

		try {
			const result = await this.request(
				{
					args: this.encodeValue(args, {
						callScopedCallbacks: callScoped,
					}) as unknown[],
					command: "call",
					id: requestId,
					method,
					objectId,
				},
				this.callTimeoutMs,
				method,
				{ actorId, objectId },
			);

			this.onDebug?.({
				actorId,
				durationMs: Math.round((performance.now() - started) * 1000) / 1000,
				method,
				objectId,
				requestId,
				type: "call:end",
				workerId: this.id,
			});

			return result;
		} catch (err) {
			this.onDebug?.({
				actorId,
				durationMs: Math.round((performance.now() - started) * 1000) / 1000,
				error: err instanceof Error ? err.message : String(err),
				method,
				objectId,
				requestId,
				type: "call:end",
				workerId: this.id,
			});
			throw err;
		} finally {
			this.hostCallbacks.releaseCallScoped(callScoped);
		}
	}

	/**
	 * Sends a {@link ProtocolMessage} and waits for the matching response.
	 * @param message - Outbound command (must carry a unique `id`)
	 * @param timeoutMs - Optional call timeout
	 * @param methodForTimeout - Method name for timeout error text / debug
	 * @param timeoutMeta - Actor identity for `call:timeout` debug events
	 * @returns Decoded response value
	 */
	private request(
		message: ProtocolMessage,
		timeoutMs?: number,
		methodForTimeout?: string,
		timeoutMeta?: { actorId: string; objectId: number; },
	): Promise<unknown> {
		return new Promise((resolve, reject) => {
			let timer: ReturnType<typeof setTimeout> | undefined;
			const clear = () => {
				if (timer) clearTimeout(timer);
			};

			this.pending.set(message.id, {
				reject: (reason) => {
					clear();
					reject(reason);
				},
				resolve: (value) => {
					clear();
					resolve(value);
				},
			});

			if (timeoutMs !== undefined && timeoutMs > 0) {
				timer = setTimeout(() => {
					if (!this.pending.has(message.id)) return;
					this.pending.delete(message.id);
					this.notifyDrain();
					this.onDebug?.({
						...(timeoutMeta?.actorId
							? { actorId: timeoutMeta.actorId }
							: {}),
						...(timeoutMeta?.objectId !== undefined
							? { objectId: timeoutMeta.objectId }
							: {}),
						...(methodForTimeout ? { method: methodForTimeout } : {}),
						requestId: message.id,
						timeoutMs,
						type: "call:timeout",
						workerId: this.id,
					});
					reject(
						new Error(
							`Call timed out after ${timeoutMs}ms`
							+ (methodForTimeout ? `: ${methodForTimeout}` : ""),
						),
					);
				}, timeoutMs);
			}

			this.post(message);
		});
	}

	/**
	 * `postMessage` with a clearer error when structured clone fails.
	 * @param message - Payload to send into the worker
	 */
	private post(message: unknown): void {
		try {
			this.worker.postMessage(message);
		} catch (err) {
			throw new Error(cloneErrorMessage(err));
		}
	}

	/** Wakes {@link drain} waiters when no requests are pending. */
	private notifyDrain(): void {
		if (this.pending.size > 0) return;
		const waiters = this.drainWaiters;

		this.drainWaiters = [];
		for (const wait of waiters) wait();
	}

	/** Waits until all in-flight requests on this worker have settled. */
	async drain(): Promise<void> {
		if (this.pending.size === 0) return;
		await new Promise<void>((resolve) => {
			this.drainWaiters.push(resolve);
		});
	}

	/**
	 * Asks the worker to call dispose/close on every live actor (best-effort).
	 */
	async closeAllActors(): Promise<void> {
		if (this.closed) return;
		await this.request({
			command: "close_all",
			id: this.nextRequestId++,
		});
	}

	/**
	 * Closes the node: optional actor close, drain, reject pendings, terminate thread.
	 * @param options - `{ closeActors: false }` skips per-actor close
	 */
	async gracefulTerminate(options?: {
		closeActors?: boolean;
	}): Promise<void> {
		this.closed = true;
		try {
			if (options?.closeActors !== false) {
				await this.closeAllActors();
			}
			await this.drain();
		} finally {
			for (const [, p] of this.pending) {
				p.reject(new Error("Runtime is disposed"));
			}
			this.pending.clear();
			await this.worker.terminate();
		}
	}
}
