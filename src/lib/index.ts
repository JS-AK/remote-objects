/**
 * Public API for `@js-ak/remote-objects`.
 *
 * Actor-style remote objects on Node.js worker threads — write normal classes,
 * bind them with {@link actor}, spawn via {@link Runtime}, call through typed proxies.
 */
export type {
	ActorClass,
	ActorHandle,
	ActorProxy,
	AnyActorClass,
	DebugEvent,
	DebugHandler,
	DestroyOptions,
	RuntimeDebug,
	RuntimeOptions,
} from "./types.js";
export type { ActorMeta, ActorModuleRef } from "./actor-meta.js";
export { Runtime } from "./runtime/runtime.js";

export { actor } from "./actor-meta.js";
export { getActorHandle } from "./proxy/proxy.js";
