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
	/** Reverse index: actor objectId → callback ids bound to it. */
	private readonly boundByObject = new Map<number, Set<number>>();
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
		if (entry.boundObjectId !== undefined) {
			this.trackBound(entry.boundObjectId, callbackId);
		}

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
			this.removeEntry(id);
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
				this.removeEntry(id);
			}
		}
	}

	/**
	 * Drops callbacks returned by a given actor (on destroy).
	 * Uses a reverse index for O(k) cleanup where k is callbacks bound to the actor.
	 * @param objectId - Actor that owned the returned callbacks
	 */
	releaseBoundToObject(objectId: number): void {
		const ids = this.boundByObject.get(objectId);

		if (!ids) return;
		for (const id of ids) {
			this.entries.delete(id);
		}
		this.boundByObject.delete(objectId);
	}

	/** Removes every registered callback. */
	clear(): void {
		this.entries.clear();
		this.boundByObject.clear();
	}

	/**
	 * Drops one entry and keeps {@link boundByObject} in sync.
	 * @param callbackId - Id to remove
	 */
	private removeEntry(callbackId: number): void {
		const entry = this.entries.get(callbackId);

		if (!entry) return;
		if (entry.boundObjectId !== undefined) {
			this.untrackBound(entry.boundObjectId, callbackId);
		}
		this.entries.delete(callbackId);
	}

	/**
	 * @param objectId - Actor that owns returned callbacks
	 * @param callbackId - Registered callback id
	 */
	private trackBound(objectId: number, callbackId: number): void {
		let ids = this.boundByObject.get(objectId);

		if (!ids) {
			ids = new Set();
			this.boundByObject.set(objectId, ids);
		}
		ids.add(callbackId);
	}

	/**
	 * @param objectId - Actor that owned the callback
	 * @param callbackId - Registered callback id
	 */
	private untrackBound(objectId: number, callbackId: number): void {
		const ids = this.boundByObject.get(objectId);

		if (!ids) return;
		ids.delete(callbackId);
		if (ids.size === 0) {
			this.boundByObject.delete(objectId);
		}
	}
}
