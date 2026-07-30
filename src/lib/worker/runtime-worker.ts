/**
 * Worker-thread entry for `@js-ak/remote-objects`.
 *
 * Loads actor modules, hosts sticky instances, encodes/decodes wire values
 * (actors, callbacks, streams), and talks to the parent {@link WorkerNode}
 * via protocol + bridge messages.
 *
 * Always loaded as ESM (`build/esm/lib/worker/runtime-worker.js`) even when
 * the host package is consumed as CJS.
 */
/* eslint-disable sort-imports */
import { parentPort, workerData } from "node:worker_threads";
import { pathToFileURL } from "node:url";

import type {
	ActorRef,
	BridgeCallMessage,
	BridgeCallbackInvokeMessage,
	BridgeCallbackResultMessage,
	BridgeResultMessage,
	BridgeStreamMessage,
	CallbackRef,
	ProtocolMessage,
	StreamRef,
} from "../protocol/messages.js";
import type { AnyActorClass } from "../types.js";
import { fromWireError, toWireError } from "../protocol/messages.js";
import { actorRef } from "../protocol/refs.js";
import { Serializer } from "../protocol/serializer.js";
import { CallbackRegistry } from "../protocol/callback-registry.js";
import { StreamBridge, type StreamTransport } from "../protocol/stream-bridge.js";
import { cloneErrorMessage } from "../protocol/plain.js";

if (!parentPort) {
	throw new Error("runtime-worker must run inside a Worker");
}

const port = parentPort;
const workerId = (workerData as { workerId: number; }).workerId;
const registry = new Map<string, AnyActorClass>();
const objects = new Map<number, object>();
const objectIds = new Map<object, number>();
const stubRefs = new WeakMap<object, ActorRef>();
const serializer = new Serializer();
const callbacks = new CallbackRegistry(workerId);
let nextBridgeId = 1;
const bridgePending = new Map<
	number,
	{
		reject: (reason?: unknown) => void;
		resolve: (value: unknown) => void;
	}
>();

/**
 * Posts to the parent port with a clearer error on structured-clone failure.
 * @param message - Protocol or bridge payload
 */
function post(message: unknown): void {
	try {
		port.postMessage(message);
	} catch (err) {
		throw new Error(cloneErrorMessage(err));
	}
}

/**
 * Builds worker-side {@link StreamTransport} that posts bridge stream messages to the host.
 * @param ref - Stream being transported from this worker
 * @returns Stream transport that posts bridge stream messages
 */
function createStreamTransport(ref: StreamRef): StreamTransport {
	return {
		sendClose: () => {
			post({
				bridge: "stream_close",
				owner: ref.owner,
				streamId: ref.streamId,
			} satisfies BridgeStreamMessage);
		},
		sendData: (chunk) => {
			post({
				bridge: "stream_data",
				chunk,
				direction: "from_owner",
				owner: ref.owner,
				streamId: ref.streamId,
			} satisfies BridgeStreamMessage);
		},
		sendEnd: () => {
			post({
				bridge: "stream_end",
				direction: "from_owner",
				owner: ref.owner,
				streamId: ref.streamId,
			} satisfies BridgeStreamMessage);
		},
		sendError: (err) => {
			post({
				bridge: "stream_error",
				direction: "from_owner",
				error: toWireError(err),
				owner: ref.owner,
				streamId: ref.streamId,
			} satisfies BridgeStreamMessage);
		},
		sendPause: () => {
			post({
				bridge: "stream_pause",
				owner: ref.owner,
				streamId: ref.streamId,
			} satisfies BridgeStreamMessage);
		},
		sendResume: () => {
			post({
				bridge: "stream_resume",
				owner: ref.owner,
				streamId: ref.streamId,
			} satisfies BridgeStreamMessage);
		},
		sendWrite: (id, chunk) => {
			post({
				bridge: "stream_write",
				chunk,
				id,
				owner: ref.owner,
				streamId: ref.streamId,
			} satisfies BridgeStreamMessage);
		},
		sendWriteEnd: (id) => {
			post({
				bridge: "stream_write_end",
				id,
				owner: ref.owner,
				streamId: ref.streamId,
			} satisfies BridgeStreamMessage);
		},
		sendWriteResult: (id, error) => {
			post({
				bridge: "stream_write_result",
				id,
				owner: ref.owner,
				streamId: ref.streamId,
				...(error ? { error: toWireError(error) } : {}),
			} satisfies BridgeStreamMessage);
		},
	};
}

const streams = new StreamBridge(workerId, createStreamTransport);

/**
 * Normalizes a module path to a URL suitable for dynamic `import()`.
 * @param moduleUrl - Absolute path or already-qualified `file:` / `data:` URL
 * @returns URL string suitable for dynamic `import()`
 */
function toImportUrl(moduleUrl: string): string {
	if (moduleUrl.startsWith("file:") || moduleUrl.startsWith("data:")) {
		return moduleUrl;
	}

	return pathToFileURL(moduleUrl).href;
}

/**
 * Resolves a named (or default) class export from a loaded actor module.
 * @param mod - Module namespace object from `import()`
 * @param exportName - Expected export name
 * @param moduleUrl - For error messages
 * @returns Actor class export
 */
function resolveExport(
	mod: Record<string, unknown>,
	exportName: string,
	moduleUrl: string,
): AnyActorClass {
	const direct = mod[exportName];

	if (typeof direct === "function") {
		return direct as AnyActorClass;
	}

	const defaultExport = mod.default;

	if (typeof defaultExport === "function") {
		if (
			exportName === "default"
			|| (defaultExport as { name?: string; }).name === exportName
		) {
			return defaultExport as AnyActorClass;
		}
	}

	const keys = Object.keys(mod).join(", ") || "(none)";

	throw new Error(
		`Export "${exportName}" not found in ${moduleUrl}. Available: ${keys}`,
	);
}

/**
 * Encodes a worker-local value for the wire (actors, stubs, callbacks, streams).
 * @param value - Return value or argument tree
 * @param options - Current actor context and optional call-scoped callback list
 * @returns Wire-safe encoded value
 */
function encodeValue(
	value: unknown,
	options?: {
		currentObject?: object;
		currentObjectId?: number;
		callScopedCallbacks?: number[];
		boundObjectId?: number;
	},
): unknown {
	return serializer.encode(value, {
		actors: objectIds,
		...(options?.currentObject !== undefined
			? { currentObject: options.currentObject }
			: {}),
		...(options?.currentObjectId !== undefined
			? { currentObjectId: options.currentObjectId }
			: {}),
		registerCallback: (fn) => {
			const ref = callbacks.register(fn as (...args: unknown[]) => unknown, {
				callScoped: options?.callScopedCallbacks !== undefined,
				...(options?.boundObjectId !== undefined
					? { boundObjectId: options.boundObjectId }
					: options?.currentObjectId !== undefined
						? { boundObjectId: options.currentObjectId }
						: {}),
			});

			options?.callScopedCallbacks?.push(ref.callbackId);

			return ref;
		},
		registerStream: (obj) => streams.tryRegisterLocal(obj),
		resolveProxy: (value) => {
			const ref = stubRefs.get(value as object);

			if (!ref) return undefined;

			return { objectId: ref.objectId, workerId: ref.workerId };
		},
		workerId,
	});
}

/**
 * Builds an async stub for a callback owned elsewhere (host or another worker).
 * @param ref - Wire callback tag
 * @returns Local invoker or async bridge stub for the callback
 */
function resolveCallbackRef(ref: CallbackRef): unknown {
	if (ref.owner === workerId) {
		return (...args: unknown[]) => callbacks.invoke(ref.callbackId, args);
	}

	return (...args: unknown[]) => {
		const id = nextBridgeId++;
		const encodedArgs = encodeValue(args);

		return new Promise((resolve, reject) => {
			bridgePending.set(id, { reject, resolve });
			const msg: BridgeCallbackInvokeMessage = {
				args: encodedArgs as unknown[],
				bridge: "callback_invoke",
				callbackId: ref.callbackId,
				id,
				owner: ref.owner,
			};

			post(msg);
		});
	};
}

/**
 * Creates a local stream proxy for a remote {@link StreamRef}.
 * @param ref - Wire stream tag
 * @returns Local stream proxy for the remote stream
 */
function resolveStreamRef(ref: StreamRef): unknown {
	return streams.createProxy(ref);
}

/**
 * Decodes a wire value into local actors, stubs, callbacks, or stream proxies.
 * @param value - Encoded wire value
 * @returns Local actors, stubs, callbacks, or stream proxies
 */
function decodeValue(value: unknown): unknown {
	return serializer.decode(value, {
		resolveActorRef: (ref) => decodeActorRef(ref),
		resolveCallbackRef: (ref) => resolveCallbackRef(ref),
		resolveStreamRef: (ref) => resolveStreamRef(ref),
	});
}

/**
 * Resolves an actor ref to a local instance or a cross-worker stub.
 * @param ref - Actor identity from the wire
 * @returns Local actor instance or cross-worker stub
 */
function decodeActorRef(ref: {
	workerId: number;
	objectId: number;
}): unknown {
	if (ref.workerId === workerId) {
		const local = objects.get(ref.objectId);

		if (!local) {
			throw new Error(`Unknown local actor ${workerId}:${ref.objectId}`);
		}

		return local;
	}

	return createRemoteStub(ref.workerId, ref.objectId);
}

/**
 * Proxy stub for an actor living on another worker; calls go through the bridge.
 * @param targetWorkerId - Worker that owns the actor
 * @param objectId - Object id on that worker
 * @returns Proxy stub that bridges method calls to the remote actor
 */
function createRemoteStub(targetWorkerId: number, objectId: number): object {
	const stub = new Proxy(
		{},
		{
			get(_target, prop) {
				if (typeof prop === "symbol" || prop === "then") {
					return undefined;
				}

				return (...args: unknown[]) => {
					const id = nextBridgeId++;
					const encodedArgs = encodeValue(args);

					return new Promise((resolve, reject) => {
						bridgePending.set(id, { reject, resolve });
						const msg: BridgeCallMessage = {
							args: encodedArgs as unknown[],
							bridge: "call",
							id,
							method: prop,
							objectId,
							targetWorkerId,
						};

						post(msg);
					});
				};
			},
		},
	);

	stubRefs.set(stub, actorRef(targetWorkerId, objectId));

	return stub;
}

/**
 * Best-effort call to instance `dispose` or `close` if present.
 * @param instance - Actor instance
 */
async function closeActor(instance: object): Promise<void> {
	const record = instance as Record<string, unknown>;
	const method
		= typeof record.dispose === "function"
			? "dispose"
			: typeof record.close === "function"
				? "close"
				: null;

	if (!method) return;
	await (record[method] as () => unknown).call(instance);
}

/**
 * Applies an inbound stream bridge message to the local {@link StreamBridge}.
 * @param msg - Stream control/data message from the host
 */
async function handleStreamMessage(msg: BridgeStreamMessage): Promise<void> {
	const { owner, streamId } = msg;

	if (msg.bridge === "stream_data" && msg.direction === "from_owner") {
		streams.onRemoteData(owner, streamId, msg.chunk);

		return;
	}

	if (msg.bridge === "stream_end" && msg.direction === "from_owner") {
		streams.onRemoteEnd(owner, streamId);

		return;
	}

	if (msg.bridge === "stream_error") {
		streams.onRemoteError(owner, streamId, fromWireError(msg.error));

		return;
	}

	if (msg.bridge === "stream_pause") {
		streams.onPause(owner, streamId);

		return;
	}

	if (msg.bridge === "stream_resume") {
		streams.onResume(owner, streamId);

		return;
	}

	if (msg.bridge === "stream_write") {
		await streams.onWrite(owner, streamId, msg.id, msg.chunk);

		return;
	}

	if (msg.bridge === "stream_write_end") {
		await streams.onWriteEnd(owner, streamId, msg.id);

		return;
	}

	if (msg.bridge === "stream_write_result") {
		streams.onWriteResult(
			owner,
			streamId,
			msg.id,
			msg.error ? fromWireError(msg.error) : undefined,
		);

		return;
	}

	if (msg.bridge === "stream_close") {
		streams.onClose(owner, streamId);
	}
}

port.on(
	"message",
	async (
		msg:
			| ProtocolMessage
			| BridgeResultMessage
			| BridgeCallbackResultMessage
			| BridgeStreamMessage,
	) => {
		if (
			typeof msg === "object"
			&& msg !== null
			&& "bridge" in msg
		) {
			if (msg.bridge === "result") {
				const pending = bridgePending.get(msg.id);

				if (!pending) return;
				bridgePending.delete(msg.id);

				if ("error" in msg) {
					pending.reject(fromWireError(msg.error));

					return;
				}

				pending.resolve(decodeValue(msg.result));

				return;
			}

			if (msg.bridge === "callback_result") {
				const pending = bridgePending.get(msg.id);

				if (!pending) return;
				bridgePending.delete(msg.id);

				if ("error" in msg) {
					pending.reject(fromWireError(msg.error));

					return;
				}

				pending.resolve(decodeValue(msg.result));

				return;
			}

			if (typeof msg.bridge === "string" && msg.bridge.startsWith("stream_")) {
				await handleStreamMessage(msg as BridgeStreamMessage);

				return;
			}

			return;
		}

		const request = msg as ProtocolMessage;

		try {
			if (request.command === "register") {
				const mod = (await import(toImportUrl(request.moduleUrl))) as Record<
					string,
					unknown
				>;
				const Class = resolveExport(
					mod,
					request.exportName,
					request.moduleUrl,
				);

				registry.set(request.className, Class);
				post({ id: request.id, result: null });

				return;
			}

			if (request.command === "create") {
				const Class = registry.get(request.className);

				if (!Class) {
					throw new Error(`Unknown class: ${request.className}`);
				}

				const args = decodeValue(request.args) as unknown[];
				const instance = new Class(...args);

				objects.set(request.objectId, instance);
				objectIds.set(instance, request.objectId);

				post({ id: request.id, result: null });

				return;
			}

			if (request.command === "destroy") {
				const instance = objects.get(request.objectId);

				if (instance) {
					if (request.close !== false) {
						try {
							await closeActor(instance);
						} catch {
							// Best-effort close before destroy.
						}
					}
					objectIds.delete(instance);
					objects.delete(request.objectId);
					callbacks.releaseBoundToObject(request.objectId);
				}
				post({ id: request.id, result: null });

				return;
			}

			if (request.command === "close_all") {
				const instances = [...objects.values()];

				for (const instance of instances) {
					try {
						await closeActor(instance);
					} catch {
						// Best-effort cleanup during graceful shutdown.
					}
				}
				streams.closeAll();
				callbacks.clear();
				post({ id: request.id, result: null });

				return;
			}

			if (request.command === "callback_invoke") {
				const args = decodeValue(request.args) as unknown[];
				const result = await callbacks.invoke(request.callbackId, args);

				post({
					id: request.id,
					result: encodeValue(result),
				});

				return;
			}

			if (request.command === "callback_release") {
				callbacks.release(request.callbackIds);
				post({ id: request.id, result: null });

				return;
			}

			if (request.command === "call") {
				const object = objects.get(request.objectId);

				if (!object) {
					throw new Error(`Unknown object: ${request.objectId}`);
				}

				const fn = (object as Record<string, unknown>)[request.method];

				if (typeof fn !== "function") {
					throw new Error(`Unknown method: ${request.method}`);
				}

				const args = decodeValue(request.args) as unknown[];
				const result = await fn.apply(object, args);
				const encoded = encodeValue(result, {
					boundObjectId: request.objectId,
					currentObject: object,
					currentObjectId: request.objectId,
				});

				post({ id: request.id, result: encoded });

				return;
			}

			throw new Error("Unknown command");
		} catch (err) {
			post({
				error: toWireError(err),
				id: request.id,
			});
		}
	},
);
