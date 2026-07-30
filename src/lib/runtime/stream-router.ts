/**
 * Tracks which sides hold a proxy for a given stream_ref.
 * Used by the host to fan-out `stream_data` / write acks to the right consumers.
 */
export class StreamRouter {
	private readonly subs = new Map<string, Set<"host" | number>>();

	/**
	 * Composite key for owner + streamId.
	 * @param owner - `"host"` or worker id that owns the real stream
	 * @param streamId - Stream id on that owner
	 * @returns Map key string
	 */
	private key(owner: "host" | number, streamId: number): string {
		return `${owner}:${streamId}`;
	}

	/**
	 * Records that `consumer` holds a proxy for the given stream.
	 * @param owner - Stream owner
	 * @param streamId - Stream id
	 * @param consumer - `"host"` or worker id that should receive events
	 */
	subscribe(
		owner: "host" | number,
		streamId: number,
		consumer: "host" | number,
	): void {
		const key = this.key(owner, streamId);
		let set = this.subs.get(key);

		if (!set) {
			set = new Set();
			this.subs.set(key, set);
		}
		set.add(consumer);
	}

	/**
	 * @param owner - Stream owner
	 * @param streamId - Stream id
	 * @returns All consumers currently subscribed to this stream
	 */
	consumers(
		owner: "host" | number,
		streamId: number,
	): Array<"host" | number> {
		return [...(this.subs.get(this.key(owner, streamId)) ?? [])];
	}

	/**
	 * Drops all consumers for a stream (on close).
	 * @param owner - Stream owner
	 * @param streamId - Stream id
	 */
	unsubscribeAll(owner: "host" | number, streamId: number): void {
		this.subs.delete(this.key(owner, streamId));
	}

	/** Clears every subscription (runtime dispose). */
	clear(): void {
		this.subs.clear();
	}
}
