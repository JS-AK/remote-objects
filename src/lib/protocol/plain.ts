/**
 * Builds a clear error message when `postMessage` / structured clone fails.
 * @param err - Underlying clone/postMessage error
 * @returns Human-readable error string for wrapping
 */
export function cloneErrorMessage(err: unknown): string {
	const message = err instanceof Error ? err.message : String(err);

	return (
		"Value is not structured-clone compatible for worker_threads "
		+ `(and is not an actor/callback/stream ref). ${message}`
	);
}

/**
 * True for JSON-like plain objects (not class instances, arrays, or null).
 * Used by the serializer to decide what to deep-walk.
 * @param value - Value under test
 * @returns Whether `value` is a plain object
 */
export function isPlainObject(value: unknown): value is Record<string, unknown> {
	if (typeof value !== "object" || value === null) return false;
	const proto = Object.getPrototypeOf(value);

	return proto === Object.prototype || proto === null;
}
