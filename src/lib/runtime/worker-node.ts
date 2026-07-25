import { Worker } from "node:worker_threads";
import path from "node:path";

import type { ActorHandle, DebugHandler } from "../types.js";
import type {
	ActorRef,
	BridgeCallMessage,
	BridgeResultMessage,
	ProtocolMessage,
	ProtocolResponse,
} from "../protocol/messages.js";
import {
	actorRef, formatActorId, isActorRef,
} from "../protocol/refs.js";
import { createProxy, getActorHandle } from "../proxy/proxy.js";
import { Serializer } from "../protocol/serializer.js";
import { getModuleDir } from "./module-dir.js";

type Pending = {
	reject: (reason?: unknown) => void;
	resolve: (value: unknown) => void;
};

export type WorkerNodeOptions = {
	id: number;
	onDebug?: DebugHandler;
	callTimeoutMs?: number;
	getWorker: (workerId: number) => WorkerNode | undefined;
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

export class WorkerNode {
	readonly id: number;
	private readonly worker: Worker;
	private readonly pending = new Map<number, Pending>();
	private readonly serializer = new Serializer();
	private readonly onDebug?: DebugHandler;
	private readonly callTimeoutMs?: number;
	private readonly getWorker: (workerId: number) => WorkerNode | undefined;
	private nextRequestId = 1;
	private nextObjectId = 1;
	private closed = false;
	private readonly localObjects = new Set<number>();
	/** Per-actor mailbox: serialize calls to the same objectId. */
	private readonly mailboxes = new Map<number, Promise<unknown>>();
	private drainWaiters: Array<() => void> = [];

	constructor(options: WorkerNodeOptions) {
		this.id = options.id;
		this.getWorker = options.getWorker;
		if (options.onDebug) this.onDebug = options.onDebug;
		if (options.callTimeoutMs !== undefined) {
			this.callTimeoutMs = options.callTimeoutMs;
		}

		this.worker = new Worker(workerEntry, {
			workerData: { workerId: this.id },
		});

		this.worker.on("message", (msg: ProtocolResponse | BridgeCallMessage) => {
			if (
				typeof msg === "object"
				&& msg !== null
				&& "bridge" in msg
				&& msg.bridge === "call"
			) {
				void this.handleBridgeCall(msg);

				return;
			}

			const response = msg as ProtocolResponse;
			const pending = this.pending.get(response.id);

			if (!pending) return;
			this.pending.delete(response.id);
			this.notifyDrain();

			if ("error" in response) {
				const err = new Error(response.error.message);

				if (response.error.name) err.name = response.error.name;
				if (response.error.stack) err.stack = response.error.stack;
				pending.reject(err);

				return;
			}

			pending.resolve(
				this.serializer.decode(response.result, (ref) =>
					this.resolveActorRef(ref),
				),
			);
		});

		this.worker.on("error", (err) => {
			for (const [, p] of this.pending) {
				p.reject(err);
			}
			this.pending.clear();
			this.notifyDrain();
		});
	}

	private resolveActorRef(ref: ActorRef): unknown {
		const target = this.getWorker(ref.workerId);

		if (!target) {
			throw new Error(
				`Unknown worker ${ref.workerId} for actor ${formatActorId(ref.workerId, ref.objectId)}`,
			);
		}

		return target.createLocalProxy(ref.objectId);
	}

	createLocalProxy<T extends object>(objectId: number): T {
		const handle: ActorHandle = { objectId, workerId: this.id };

		return createProxy(handle, (id, method, methodArgs) =>
			this.call(id, method, methodArgs),
		) as T;
	}

	private encodeArgs(args: unknown[]): unknown[] {
		return args.map((arg) => {
			const handle = getActorHandle(arg);

			if (handle) {
				return actorRef(handle.workerId, handle.objectId);
			}

			return arg;
		});
	}

	private async handleBridgeCall(msg: BridgeCallMessage): Promise<void> {
		try {
			const target = this.getWorker(msg.targetWorkerId);

			if (!target) {
				throw new Error(`Unknown worker ${msg.targetWorkerId}`);
			}

			const decodedArgs = msg.args.map((arg) => {
				if (isActorRef(arg)) {
					return this.resolveActorRef(arg);
				}

				return arg;
			});

			const result = await target.call(msg.objectId, msg.method, decodedArgs);
			const handle = getActorHandle(result);
			const encoded = handle
				? actorRef(handle.workerId, handle.objectId)
				: result;

			const reply: BridgeResultMessage = {
				bridge: "result",
				id: msg.id,
				result: encoded,
			};

			this.worker.postMessage(reply);
		} catch (err) {
			const error = err instanceof Error ? err : new Error(String(err));
			const reply: BridgeResultMessage = {
				bridge: "result",
				error: {
					message: error.message,
					name: error.name,
					...(error.stack ? { stack: error.stack } : {}),
				},
				id: msg.id,
			};

			this.worker.postMessage(reply);
		}
	}

	assertOpen(action: string): void {
		if (this.closed) {
			throw new Error(`Runtime is disposed; cannot ${action}`);
		}
	}

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

	async create<T extends object>(
		className: string,
		args: unknown[],
	): Promise<T> {
		this.assertOpen("spawn");
		const objectId = this.nextObjectId++;

		this.localObjects.add(objectId);

		await this.request({
			args: this.encodeArgs(args),
			className,
			command: "create",
			id: this.nextRequestId++,
			objectId,
		});

		this.onDebug?.({
			actorId: formatActorId(this.id, objectId),
			className,
			objectId,
			type: "spawn",
			workerId: this.id,
		});

		return this.createLocalProxy(objectId);
	}

	async destroy(objectId: number): Promise<void> {
		this.assertOpen("destroy");
		if (!this.localObjects.has(objectId)) {
			throw new Error(
				`Unknown actor ${formatActorId(this.id, objectId)} on worker ${this.id}`,
			);
		}

		await this.enqueue(objectId, async () => {
			await this.request({
				command: "destroy",
				id: this.nextRequestId++,
				objectId,
			});
		});

		this.localObjects.delete(objectId);
		this.mailboxes.delete(objectId);

		this.onDebug?.({
			actorId: formatActorId(this.id, objectId),
			objectId,
			type: "destroy",
			workerId: this.id,
		});
	}

	async call(
		objectId: number,
		method: string,
		args: unknown[],
	): Promise<unknown> {
		this.assertOpen(`call ${method}`);

		return this.enqueue(objectId, () => this.doCall(objectId, method, args));
	}

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

	private async doCall(
		objectId: number,
		method: string,
		args: unknown[],
	): Promise<unknown> {
		const requestId = this.nextRequestId++;
		const started = performance.now();
		const actorId = formatActorId(this.id, objectId);

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
					args: this.encodeArgs(args),
					command: "call",
					id: requestId,
					method,
					objectId,
				},
				this.callTimeoutMs,
				method,
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
		}
	}

	private request(
		message: ProtocolMessage,
		timeoutMs?: number,
		methodForTimeout?: string,
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
					reject(
						new Error(
							`Call timed out after ${timeoutMs}ms`
							+ (methodForTimeout ? `: ${methodForTimeout}` : ""),
						),
					);
				}, timeoutMs);
			}

			this.worker.postMessage(message);
		});
	}

	private notifyDrain(): void {
		if (this.pending.size > 0) return;
		const waiters = this.drainWaiters;

		this.drainWaiters = [];
		for (const wait of waiters) wait();
	}

	async drain(): Promise<void> {
		if (this.pending.size === 0) return;
		await new Promise<void>((resolve) => {
			this.drainWaiters.push(resolve);
		});
	}

	async closeAllActors(): Promise<void> {
		if (this.closed) return;
		await this.request({
			command: "close_all",
			id: this.nextRequestId++,
		});
	}

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
