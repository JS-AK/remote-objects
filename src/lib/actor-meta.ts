import path from "node:path";
import { pathToFileURL } from "node:url";

import type { AnyActorClass } from "./types.js";

/** Actor class with optional module metadata attached by {@link actor}. */
export type ActorClassWithMeta = AnyActorClass & {
	[ACTOR_META]?: ActorMeta;
};

/** Module location + export name needed to load a class inside a worker. */
export type ActorMeta = {
	moduleUrl: string;
	exportName: string;
};

/**
 * Module location accepted by {@link actor}.
 * Use `import.meta` (ESM), `__filename` / string path (CJS), or `{ url }` / `{ filename }`.
 */
export type ActorModuleRef =
  | ImportMeta
  | { url: string; }
  | { filename: string; }
  | string;

/** Well-known symbol storing {@link ActorMeta} on a bound class. */
export const ACTOR_META = Symbol.for("remote-objects.actorMeta");

/**
 * Resolves a module location to a `file:` / `data:` URL for worker `import()`.
 * @param meta - `import.meta`, `__filename`, path string, or `{ url }` / `{ filename }`
 * @returns Absolute module URL
 */
function resolveModuleUrl(meta: ActorModuleRef): string {
	if (typeof meta === "string") {
		if (meta.startsWith("file:") || meta.startsWith("data:")) {
			return meta;
		}

		return pathToFileURL(path.resolve(meta)).href;
	}

	if ("filename" in meta && typeof meta.filename === "string") {
		return pathToFileURL(path.resolve(meta.filename)).href;
	}

	if ("url" in meta && typeof meta.url === "string") {
		return meta.url;
	}

	throw new Error(
		"actor() needs a module location: import.meta, __filename, { url }, or { filename }",
	);
}

/**
 * Bind a class to its module so `runtime.register(Counter)` needs no URL.
 *
 * ESM:
 *   actor(Counter, import.meta);
 *
 * CJS:
 *   actor(Counter, __filename);
 *
 * @param Class - Actor class to bind
 * @param meta - Module location (`import.meta`, `__filename`, etc.)
 * @param exportName - Named export to load in the worker (default: `Class.name`)
 * @returns The same class (for chaining / re-export)
 */
export function actor<T extends AnyActorClass>(
	Class: T,
	meta: ActorModuleRef,
	exportName: string = Class.name,
): T {
	(Class as ActorClassWithMeta)[ACTOR_META] = {
		exportName,
		moduleUrl: resolveModuleUrl(meta),
	};

	return Class;
}

/**
 * Reads module metadata previously attached by {@link actor}.
 * @param Class - Actor class
 * @returns Binding metadata, or `undefined` if the class was never bound
 */
export function getActorMeta(Class: AnyActorClass): ActorMeta | undefined {
	return (Class as ActorClassWithMeta)[ACTOR_META];
}
