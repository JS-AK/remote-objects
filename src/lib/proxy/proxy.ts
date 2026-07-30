import type { ActorHandle } from "../types.js";

/**
 * Routes a proxy method invocation to the owning {@link WorkerNode}.
 * @param objectId - Actor id on that worker
 * @param method - Method name
 * @param args - Encoded-ready argument list (host still encodes before send)
 */
export type CallHandler = (
	objectId: number,
	method: string,
	args: unknown[],
) => Promise<unknown>;

const handles = new WeakMap<object, ActorHandle>();

/**
 * Builds a Proxy that looks like a local instance but routes
 * every method call through the runtime message protocol.
 *
 * @param handle - Stable actor identity
 * @param call - Handler that performs the remote call
 * @returns Opaque proxy; use {@link getActorHandle} to recover the handle
 */
export function createProxy<T extends object>(
	handle: ActorHandle,
	call: CallHandler,
): T {
	const proxy = new Proxy({} as T, {
		get(_target, prop, receiver) {
			if (typeof prop === "symbol") {
				return Reflect.get(_target, prop, receiver);
			}

			if (prop === "then") {
				// Not a thenable — avoids accidental Promise treatment.
				return undefined;
			}

			return (...args: unknown[]) => call(handle.objectId, prop, args);
		},
	});

	handles.set(proxy, handle);

	return proxy;
}

/**
 * Returns the {@link ActorHandle} for a proxy created by this runtime, if any.
 * @param value - Suspected actor proxy
 * @returns Handle, or `undefined` when `value` is not a known proxy
 */
export function getActorHandle(value: unknown): ActorHandle | undefined {
	if (typeof value !== "object" || value === null) return undefined;

	return handles.get(value);
}
