import type { ActorHandle } from "../types.js";

export type CallHandler = (
	objectId: number,
	method: string,
	args: unknown[],
) => Promise<unknown>;

const handles = new WeakMap<object, ActorHandle>();

/**
 * Builds a Proxy that looks like a local instance but routes
 * every method call through the runtime message protocol.
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

export function getActorHandle(value: unknown): ActorHandle | undefined {
	if (typeof value !== "object" || value === null) return undefined;

	return handles.get(value);
}
