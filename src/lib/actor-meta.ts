import path from "node:path";
import { pathToFileURL } from "node:url";

import type { AnyActorClass } from "./types.js";

export type ActorClassWithMeta = AnyActorClass & {
	[ACTOR_META]?: ActorMeta;
};

export type ActorMeta = {
	moduleUrl: string;
	exportName: string;
};

export type ActorModuleRef =
  | ImportMeta
  | { url: string; }
  | { filename: string; }
  | string;

export const ACTOR_META = Symbol.for("remote-objects.actorMeta");

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

export function getActorMeta(Class: AnyActorClass): ActorMeta | undefined {
	return (Class as ActorClassWithMeta)[ACTOR_META];
}
