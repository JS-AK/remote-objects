/** Contract for classes that can live as remote actors. */
export type ActorClass<TInstance extends object = object> = {
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	new(...args: any[]): TInstance;
	name: string;
};

/** Stable identity of an actor across the runtime. */
export type ActorHandle = {
	workerId: number;
	objectId: number;
};

/**
 * Remote view of an actor: methods are async, `return this` stays a proxy.
 *
 * Already-async methods are kept as-is so generic type parameters survive
 * (mapped `infer` would erase them). Sync methods are wrapped in `Promise`.
 */
export type ActorProxy<TInstance extends object> = {
	[K in keyof TInstance as TInstance[K] extends (...args: never) => unknown
		? K
		: never]: TInstance[K] extends (...args: never) => infer R
		? R extends Promise<infer U>
			? U extends TInstance
				? TInstance[K] extends (...args: infer A) => unknown
					? (...args: A) => Promise<ActorProxy<TInstance>>
					: never
				: TInstance[K]
			: TInstance[K] extends (...args: infer A) => infer SR
				? (...args: A) => Promise<RemoteValue<TInstance, SR>>
				: never
		: never;
};

/** Any constructable actor class (preserves ctor arg types). */
export type AnyActorClass = {
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	new(...args: any[]): object;
	name: string;
};

export type DebugEvent =
	| {
		type: "register";
		workerId: number;
		className: string;
	}
	| {
		type: "spawn";
		workerId: number;
		className: string;
		objectId: number;
		actorId: string;
	}
	| {
		type: "destroy";
		workerId: number;
		objectId: number;
		actorId: string;
	}
	| {
		type: "call:start";
		workerId: number;
		objectId: number;
		actorId: string;
		method: string;
		requestId: number;
	}
	| {
		type: "call:end";
		workerId: number;
		objectId: number;
		actorId: string;
		method: string;
		requestId: number;
		durationMs: number;
		error?: string;
	}
	| {
		type: "dispose";
		workers: number;
	};

export type DebugHandler = (event: DebugEvent) => void;

/** If method returns `this` / same instance, proxy keeps identity. */
type RemoteValue<TInstance extends object, R> = R extends TInstance
	? ActorProxy<TInstance>
	: R;

export type RuntimeDebug = boolean | DebugHandler | { onEvent: DebugHandler; };

export interface RuntimeOptions {
	workers?: number;
	/** Log runtime events to stderr, or pass a custom handler. */
	debug?: RuntimeDebug;
	/** Fail a method call if the worker does not reply in time. */
	callTimeoutMs?: number;
}
