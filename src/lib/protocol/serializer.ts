import { actorRef, isActorRef } from "./refs.js";
import type { ActorRef } from "./messages.js";

export type EncodeContext = {
	workerId: number;
	currentObject: object;
	currentObjectId: number;
	/** Optional map of known live actors in this worker. */
	actors?: Map<object, number>;
};

/**
 * Encodes method return values before crossing the worker boundary.
 * `return this` becomes an actor_ref, not a state dump.
 */
export class Serializer {
	encode(value: unknown, context: EncodeContext): unknown {
		if (value === context.currentObject) {
			return actorRef(context.workerId, context.currentObjectId);
		}

		if (context.actors) {
			const objectId = context.actors.get(value as object);

			if (objectId !== undefined) {
				return actorRef(context.workerId, objectId);
			}
		}

		return value;
	}

	decode(
		value: unknown,
		resolveRef: (ref: ActorRef) => unknown,
	): unknown {
		if (isActorRef(value)) {
			return resolveRef(value);
		}

		return value;
	}
}
