import type { ActorRef } from "./messages.js";

export function actorRef(workerId: number, objectId: number): ActorRef {
	return { objectId, type: "actor_ref", workerId };
}

export function formatActorId(workerId: number, objectId: number): string {
	return `${workerId}:${objectId}`;
}

export function isActorRef(value: unknown): value is ActorRef {
	return (
		typeof value === "object"
		&& value !== null
		&& (value as ActorRef).type === "actor_ref"
		&& typeof (value as ActorRef).workerId === "number"
		&& typeof (value as ActorRef).objectId === "number"
	);
}
