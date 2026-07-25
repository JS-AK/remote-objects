import { parentPort, workerData } from "node:worker_threads";
import { pathToFileURL } from "node:url";

import type {
	BridgeCallMessage,
	BridgeResultMessage,
	ProtocolMessage,
} from "../protocol/messages.js";
import type { AnyActorClass } from "../types.js";
import { Serializer } from "../protocol/serializer.js";
import { isActorRef } from "../protocol/refs.js";

if (!parentPort) {
	throw new Error("runtime-worker must run inside a Worker");
}

const port = parentPort;
const workerId = (workerData as { workerId: number; }).workerId;
const registry = new Map<string, AnyActorClass>();
const objects = new Map<number, object>();
const objectIds = new Map<object, number>();
const serializer = new Serializer();
let nextBridgeId = 1;
const bridgePending = new Map<
	number,
	{
		reject: (reason?: unknown) => void;
		resolve: (value: unknown) => void;
	}
>();

function toImportUrl(moduleUrl: string): string {
	if (moduleUrl.startsWith("file:") || moduleUrl.startsWith("data:")) {
		return moduleUrl;
	}

	return pathToFileURL(moduleUrl).href;
}

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

function createRemoteStub(targetWorkerId: number, objectId: number): object {
	return new Proxy(
		{},
		{
			get(_target, prop) {
				if (typeof prop === "symbol" || prop === "then") {
					return undefined;
				}

				return (...args: unknown[]) => {
					const id = nextBridgeId++;
					const encodedArgs = args.map((arg) => {
						const localId = objectIds.get(arg as object);

						if (localId !== undefined) {
							return { objectId: localId, type: "actor_ref", workerId };
						}

						return arg;
					});

					return new Promise((resolve, reject) => {
						bridgePending.set(id, { reject, resolve });
						const msg: BridgeCallMessage = {
							args: encodedArgs,
							bridge: "call",
							id,
							method: prop,
							objectId,
							targetWorkerId,
						};

						port.postMessage(msg);
					});
				};
			},
		},
	);
}

function decodeArg(arg: unknown): unknown {
	if (!isActorRef(arg)) return arg;

	if (arg.workerId === workerId) {
		const local = objects.get(arg.objectId);

		if (!local) {
			throw new Error(`Unknown local actor ${workerId}:${arg.objectId}`);
		}

		return local;
	}

	return createRemoteStub(arg.workerId, arg.objectId);
}

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

port.on(
	"message",
	async (msg: ProtocolMessage | BridgeResultMessage) => {
		if (
			typeof msg === "object"
			&& msg !== null
			&& "bridge" in msg
			&& msg.bridge === "result"
		) {
			const pending = bridgePending.get(msg.id);

			if (!pending) return;
			bridgePending.delete(msg.id);

			if ("error" in msg) {
				const err = new Error(msg.error.message);

				if (msg.error.name) err.name = msg.error.name;
				if (msg.error.stack) err.stack = msg.error.stack;
				pending.reject(err);

				return;
			}

			pending.resolve(
				serializer.decode(msg.result, (ref) => decodeArg(ref)),
			);

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
				port.postMessage({ id: request.id, result: null });

				return;
			}

			if (request.command === "create") {
				const Class = registry.get(request.className);

				if (!Class) {
					throw new Error(`Unknown class: ${request.className}`);
				}

				const args = request.args.map(decodeArg);
				const instance = new Class(...args);

				objects.set(request.objectId, instance);
				objectIds.set(instance, request.objectId);

				port.postMessage({ id: request.id, result: null });

				return;
			}

			if (request.command === "destroy") {
				const instance = objects.get(request.objectId);

				if (instance) {
					objectIds.delete(instance);
					objects.delete(request.objectId);
				}
				port.postMessage({ id: request.id, result: null });

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
				port.postMessage({ id: request.id, result: null });

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

				const args = request.args.map(decodeArg);
				const result = await fn.apply(object, args);
				const encoded = serializer.encode(result, {
					actors: objectIds,
					currentObject: object,
					currentObjectId: request.objectId,
					workerId,
				});

				port.postMessage({ id: request.id, result: encoded });

				return;
			}

			throw new Error("Unknown command");
		} catch (err) {
			const error = err instanceof Error ? err : new Error(String(err));

			port.postMessage({
				error: {
					message: error.message,
					name: error.name,
					...(error.stack ? { stack: error.stack } : {}),
				},
				id: request.id,
			});
		}
	},
);
