import type {
	ActorRef, CallbackRef, StreamRef,
} from "./messages.js";

/**
 * Builds an {@link ActorRef} wire tag.
 * @param workerId - Worker that owns the actor
 * @param objectId - Actor id on that worker
 * @returns Wire actor reference
 */
export function actorRef(workerId: number, objectId: number): ActorRef {
	return { objectId, type: "actor_ref", workerId };
}

/**
 * Builds a {@link CallbackRef} wire tag.
 * @param owner - `"host"` or owning worker id
 * @param callbackId - Id in that side's {@link CallbackRegistry}
 * @returns Wire callback reference
 */
export function callbackRef(
	owner: "host" | number,
	callbackId: number,
): CallbackRef {
	return { callbackId, owner, type: "callback_ref" };
}

/**
 * Formats a stable actor id string (`"workerId:objectId"`).
 * @param workerId - Worker index
 * @param objectId - Object id on that worker
 * @returns Stable actor id for logs and debug events
 */
export function formatActorId(workerId: number, objectId: number): string {
	return `${workerId}:${objectId}`;
}

/**
 * Type guard for {@link ActorRef}.
 * @param value - Value under test
 * @returns Whether `value` is an actor wire tag
 */
export function isActorRef(value: unknown): value is ActorRef {
	return (
		typeof value === "object"
		&& value !== null
		&& (value as ActorRef).type === "actor_ref"
		&& typeof (value as ActorRef).workerId === "number"
		&& typeof (value as ActorRef).objectId === "number"
	);
}

/**
 * Type guard for {@link CallbackRef}.
 * @param value - Value under test
 * @returns Whether `value` is a callback wire tag
 */
export function isCallbackRef(value: unknown): value is CallbackRef {
	return (
		typeof value === "object"
		&& value !== null
		&& (value as CallbackRef).type === "callback_ref"
		&& typeof (value as CallbackRef).callbackId === "number"
		&& (
			(value as CallbackRef).owner === "host"
			|| typeof (value as CallbackRef).owner === "number"
		)
	);
}

/**
 * Type guard for {@link StreamRef}.
 * @param value - Value under test
 * @returns Whether `value` is a stream wire tag
 */
export function isStreamRef(value: unknown): value is StreamRef {
	return (
		typeof value === "object"
		&& value !== null
		&& (value as StreamRef).type === "stream_ref"
		&& typeof (value as StreamRef).streamId === "number"
		&& typeof (value as StreamRef).objectMode === "boolean"
		&& (
			(value as StreamRef).owner === "host"
			|| typeof (value as StreamRef).owner === "number"
		)
		&& (
			(value as StreamRef).mode === "readable"
			|| (value as StreamRef).mode === "writable"
			|| (value as StreamRef).mode === "duplex"
		)
	);
}

/**
 * Builds a {@link StreamRef} wire tag.
 * @param owner - `"host"` or owning worker id
 * @param streamId - Id in that side's {@link StreamBridge}
 * @param mode - Readable / writable / duplex
 * @param objectMode - Whether chunks are objects (vs Buffers/strings)
 * @returns Wire stream reference
 */
export function streamRef(
	owner: "host" | number,
	streamId: number,
	mode: StreamRef["mode"],
	objectMode: boolean,
): StreamRef {
	return {
		mode,
		objectMode,
		owner,
		streamId,
		type: "stream_ref",
	};
}
