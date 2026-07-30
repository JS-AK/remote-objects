/** Contract for classes that can live as remote actors. */
export type ActorClass<TInstance extends object = object> = {
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	new(...args: any[]): TInstance;
	name: string;
};

/** Stable identity of an actor across the runtime (`workerId:objectId`). */
export type ActorHandle = {
	workerId: number;
	objectId: number;
};

/**
 * Maps a local function type to the async stub seen across the worker boundary.
 * @internal
 */
type RemoteFunction<F> = F extends (...args: infer A) => infer R
	? (...args: A) => Promise<Awaited<R>>
	: never;

/**
 * Remote view of an actor: methods are always async, `return this` stays a proxy.
 *
 * Already-async methods are kept as-is so generic type parameters survive
 * (mapped `infer` would erase them). Sync methods are wrapped in `Promise`.
 * Returned functions become async stubs.
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

/**
 * Runtime observability events emitted when `RuntimeOptions.debug` is set.
 * Spawn/call events carry `actorId` as `"workerId:objectId"`.
 */
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
		type: "call:timeout";
		workerId: number;
		objectId?: number;
		actorId?: string;
		method?: string;
		requestId: number;
		timeoutMs: number;
	}
	| {
		type: "worker:error";
		workerId: number;
		error: string;
	}
	| {
		type: "bridge:call";
		workerId: number;
		targetWorkerId: number;
		objectId: number;
		method: string;
		requestId: number;
	}
	| {
		type: "bridge:result";
		workerId: number;
		targetWorkerId: number;
		requestId: number;
		error?: string;
	}
	| {
		type: "dispose";
		workers: number;
	};

/** Callback invoked for each {@link DebugEvent}. */
export type DebugHandler = (event: DebugEvent) => void;

/**
 * If method returns `this` / same instance, proxy keeps identity.
 * Functions become async stubs; everything else is left as-is.
 * @internal
 */
type RemoteValue<TInstance extends object, R> = R extends TInstance
	? ActorProxy<TInstance>
	: R extends (...args: never[]) => unknown
		? RemoteFunction<R>
		: R;

/** Options for {@link Runtime.destroy}. */
export type DestroyOptions = {
	/** Call actor `dispose`/`close` before removing (default true). */
	close?: boolean;
};

/**
 * Debug configuration for {@link Runtime}.
 * - `true` — log events to stderr
 * - function — custom handler
 * - `{ onEvent }` — same as function, object form
 */
export type RuntimeDebug = boolean | DebugHandler | { onEvent: DebugHandler; };

/** Options for constructing a {@link Runtime}. */
export interface RuntimeOptions {
	/** Number of worker threads in the pool (default `1`). */
	workers?: number;
	/** Log runtime events to stderr, or pass a custom handler. */
	debug?: RuntimeDebug;
	/** Fail a method call if the worker does not reply in time. */
	callTimeoutMs?: number;
}
