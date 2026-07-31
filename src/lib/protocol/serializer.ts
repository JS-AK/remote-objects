import type {
	ActorRef, CallbackRef, StreamRef,
} from "./messages.js";
import {
	isActorRef, isCallbackRef, isStreamRef,
} from "./refs.js";
import { isPlainObject } from "./plain.js";

/** Hooks used when turning wire tags back into local values / stubs. */
export type DecodeContext = {
	resolveActorRef: (ref: ActorRef) => unknown;
	resolveCallbackRef?: (ref: CallbackRef) => unknown;
	resolveStreamRef?: (ref: StreamRef) => unknown;
};

/**
 * Hooks used when turning local values into wire tags before `postMessage`.
 * Actors, callbacks, and streams are replaced with refs; arrays/plain objects are walked.
 */
export type EncodeContext = {
	workerId?: number;
	currentObject?: object;
	currentObjectId?: number;
	/** Optional map of known live actors in this worker. */
	actors?: Map<object, number>;
	/** Host-side: resolve actor proxies to handles. */
	resolveProxy?: (value: unknown) => { workerId: number; objectId: number; } | undefined;
	registerCallback?: (fn: (...args: never[]) => unknown) => CallbackRef;
	registerStream?: (value: object) => StreamRef | undefined;
};

/**
 * Encodes / decodes values crossing the worker boundary.
 * Deep-walks arrays and plain objects; tags actors, callbacks, and streams.
 */
export class Serializer {
	/**
	 * Encodes a value for the wire (deep-walk + refs).
	 * @param value - Return value or argument tree
	 * @param context - Actor / callback / stream registration hooks
	 * @returns Value safe to send via `postMessage`
	 */
	encode(value: unknown, context: EncodeContext = {}): unknown {
		return this.walkEncode(
			value,
			context,
			new WeakSet<object>(),
			new Map<object, unknown>(),
		);
	}

	/**
	 * Decodes a wire value into local actors, callback stubs, or stream proxies.
	 * @param value - Value received over `postMessage`
	 * @param context - Resolvers for each ref kind
	 * @returns Local value or stub/proxy
	 */
	decode(value: unknown, context: DecodeContext): unknown {
		return this.walkDecode(value, context, new WeakSet<object>());
	}

	/**
	 * Recursive encode walk. Throws on circular plain-object graphs.
	 * Shared (non-circular) refs are duplicated on the wire.
	 * @param value - Current node
	 * @param context - Encode hooks
	 * @param inProgress - Objects currently being walked (cycle detection)
	 * @param encoded - Finished wire copies keyed by source object
	 * @returns Encoded node
	 */
	private walkEncode(
		value: unknown,
		context: EncodeContext,
		inProgress: WeakSet<object>,
		encoded: Map<object, unknown>,
	): unknown {
		if (value === null || typeof value !== "object") {
			if (typeof value === "function") {
				if (!context.registerCallback) {
					throw new Error(
						"Functions cannot cross the worker boundary without callback support",
					);
				}

				return context.registerCallback(
					value as (...args: never[]) => unknown,
				);
			}

			return value;
		}

		if (
			context.currentObject !== undefined
			&& value === context.currentObject
			&& context.currentObjectId !== undefined
			&& context.workerId !== undefined
		) {
			return {
				objectId: context.currentObjectId,
				type: "actor_ref",
				workerId: context.workerId,
			} satisfies ActorRef;
		}

		if (context.actors) {
			const objectId = context.actors.get(value);

			if (objectId !== undefined && context.workerId !== undefined) {
				return {
					objectId,
					type: "actor_ref",
					workerId: context.workerId,
				} satisfies ActorRef;
			}
		}

		const proxyHandle = context.resolveProxy?.(value);

		if (proxyHandle) {
			return {
				objectId: proxyHandle.objectId,
				type: "actor_ref",
				workerId: proxyHandle.workerId,
			} satisfies ActorRef;
		}

		if (context.registerStream) {
			const ref = context.registerStream(value);

			if (ref) return ref;
		}

		if (inProgress.has(value)) {
			throw new Error(
				"Circular references are not supported when encoding values for remote calls",
			);
		}

		if (encoded.has(value)) {
			return structuredClone(encoded.get(value));
		}

		if (Array.isArray(value)) {
			inProgress.add(value);

			const result = value.map((item) => this.walkEncode(
				item,
				context,
				inProgress,
				encoded,
			));

			inProgress.delete(value);
			encoded.set(value, result);

			return result;
		}

		if (!isPlainObject(value)) {
			return value;
		}

		inProgress.add(value);
		const out: Record<string, unknown> = {};

		for (const [key, item] of Object.entries(value)) {
			out[key] = this.walkEncode(item, context, inProgress, encoded);
		}

		inProgress.delete(value);
		encoded.set(value, out);

		return out;
	}

	/**
	 * Recursive decode walk for arrays and plain objects.
	 * @param value - Current wire node
	 * @param context - Decode hooks
	 * @param seen - Cycle detection set
	 * @returns Decoded node
	 */
	private walkDecode(
		value: unknown,
		context: DecodeContext,
		seen: WeakSet<object>,
	): unknown {
		if (isActorRef(value)) {
			return context.resolveActorRef(value);
		}

		if (isCallbackRef(value)) {
			if (!context.resolveCallbackRef) {
				throw new Error("Callback refs are not supported in this context");
			}

			return context.resolveCallbackRef(value);
		}

		if (isStreamRef(value)) {
			if (!context.resolveStreamRef) {
				throw new Error("Stream refs are not supported in this context");
			}

			return context.resolveStreamRef(value);
		}

		if (value === null || typeof value !== "object") {
			return value;
		}

		if (seen.has(value)) {
			return value;
		}

		if (Array.isArray(value)) {
			seen.add(value);

			return value.map((item) => this.walkDecode(item, context, seen));
		}

		if (!isPlainObject(value)) {
			return value;
		}

		seen.add(value);
		const out: Record<string, unknown> = {};

		for (const [key, item] of Object.entries(value)) {
			out[key] = this.walkDecode(item, context, seen);
		}

		return out;
	}
}
