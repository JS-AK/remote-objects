import type { CallbackRef } from "./messages.js";
import { callbackRef } from "./refs.js";

/** One registered local function that was sent across the wire. */
export type CallbackEntry = {
	fn: (...args: unknown[]) => unknown;
	/** Actor that returned this callback (return-scoped). */
	boundObjectId?: number;
	/** Release after the hosting call finishes (arg-scoped). */
	callScoped?: boolean;
};

/** Side that owns the real function: main thread or a worker id. */
export type CallbackOwner = "host" | number;

/**
 * Stores local functions that were sent across the wire as callback_ref.
 */
export class CallbackRegistry {
	private readonly entries = new Map<number, CallbackEntry>();
	private nextId = 1;
	private readonly owner: CallbackOwner;

	/**
	 * @param owner - `"host"` or this worker's id (embedded in issued refs)
	 */
	constructor(owner: CallbackOwner) {
		this.owner = owner;
	}

	/**
	 * Registers a local function and returns a wire {@link CallbackRef}.
	 * @param fn - Function to invoke when the remote side calls the stub
	 * @param options - Lifetime: call-scoped and/or bound to an actor objectId
	 * @returns Wire tag for the remote side
	 */
	register(
		fn: (...args: unknown[]) => unknown,
		options?: { boundObjectId?: number; callScoped?: boolean; },
	): CallbackRef {
		const callbackId = this.nextId++;
		const entry: CallbackEntry = { fn };

		if (options?.boundObjectId !== undefined) {
			entry.boundObjectId = options.boundObjectId;
		}
		if (options?.callScoped) {
			entry.callScoped = true;
		}

		this.entries.set(callbackId, entry);

		return callbackRef(this.owner, callbackId);
	}

	/**
	 * Looks up a registered entry without invoking it.
	 * @param callbackId - Id from a {@link CallbackRef}
	 * @returns Entry, or `undefined` if unknown
	 */
	get(callbackId: number): CallbackEntry | undefined {
		return this.entries.get(callbackId);
	}

	/**
	 * Invokes a registered callback (awaits thenables).
	 * @param callbackId - Id from a {@link CallbackRef}
	 * @param args - Decoded arguments from the remote side
	 * @returns Callback result
	 */
	async invoke(callbackId: number, args: unknown[]): Promise<unknown> {
		const entry = this.entries.get(callbackId);

		if (!entry) {
			throw new Error(`Unknown callback ${callbackId}`);
		}

		return entry.fn(...args);
	}

	/**
	 * Drops the given callback ids unconditionally.
	 * @param callbackIds - Ids to remove
	 */
	release(callbackIds: Iterable<number>): void {
		for (const id of callbackIds) {
			this.entries.delete(id);
		}
	}

	/**
	 * Drops only call-scoped entries among the given ids (after a method returns).
	 * @param callbackIds - Candidate ids from the finished call
	 */
	releaseCallScoped(callbackIds: Iterable<number>): void {
		for (const id of callbackIds) {
			const entry = this.entries.get(id);

			if (entry?.callScoped) {
				this.entries.delete(id);
			}
		}
	}

	/**
	 * Drops callbacks returned by a given actor (on destroy).
	 * @param objectId - Actor that owned the returned callbacks
	 */
	releaseBoundToObject(objectId: number): void {
		for (const [id, entry] of this.entries) {
			if (entry.boundObjectId === objectId) {
				this.entries.delete(id);
			}
		}
	}

	/** Removes every registered callback. */
	clear(): void {
		this.entries.clear();
	}
}
