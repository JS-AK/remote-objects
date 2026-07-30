import {
	Duplex,
	Readable,
	Writable,
	isReadable as nodeIsReadable,
	isWritable as nodeIsWritable,
} from "node:stream";

import type { StreamRef } from "./messages.js";
import { streamRef } from "./refs.js";

/** Side that owns the real Node.js stream: main thread or a worker id. */
export type StreamOwner = "host" | number;

/**
 * Low-level send hooks used by {@link StreamBridge} to emit bridge stream messages.
 * Implemented differently on host ({@link createHostStreamTransport}) vs worker.
 */
export type StreamTransport = {
	sendData: (chunk: unknown) => void;
	sendEnd: () => void;
	sendError: (err: Error) => void;
	sendPause: () => void;
	sendResume: () => void;
	sendWrite: (id: number, chunk: unknown) => void;
	sendWriteEnd: (id: number) => void;
	sendWriteResult: (id: number, error?: Error) => void;
	sendClose: () => void;
};

/** Pending writable acknowledgements keyed by write id. */
type WritePending = Map<
	number,
	{ resolve: () => void; reject: (err: Error) => void; }
>;

/** Bookkeeping for one local or proxy stream in this bridge. */
type LocalStreamEntry = {
	ref: StreamRef;
	local: Readable | Writable | Duplex;
	proxy?: Readable | Writable | Duplex;
	writePending: WritePending;
	closed: boolean;
	/** True when we own the real stream (encoded locally). */
	isOwner: boolean;
};

/**
 * @param value - Candidate value to test
 * @returns Whether `value` is a Node.js Readable (including Duplex)
 */
function isNodeReadable(value: object): value is Readable {
	return Boolean(nodeIsReadable(value as Readable));
}

/**
 * @param value - Candidate value to test
 * @returns Whether `value` is a Node.js Writable (including Duplex)
 */
function isNodeWritable(value: object): value is Writable {
	return Boolean(nodeIsWritable(value as Writable));
}

/**
 * Bridges Node.js streams across worker_threads via stream_ref messages.
 */
export class StreamBridge {
	private readonly entries = new Map<string, LocalStreamEntry>();
	private nextId = 1;
	private readonly owner: StreamOwner;
	private readonly createTransport: (ref: StreamRef) => StreamTransport;

	/**
	 * @param owner - `"host"` or this worker's id (embedded in issued refs)
	 * @param createTransport - Factory for per-ref send hooks
	 */
	constructor(
		owner: StreamOwner,
		createTransport: (ref: StreamRef) => StreamTransport,
	) {
		this.owner = owner;
		this.createTransport = createTransport;
	}

	/**
	 * If `value` is a Node stream, registers it and returns a {@link StreamRef}.
	 * Starts forwarding owner events (`data` / `end` / `error`) over the transport.
	 * @param value - Candidate value during encode
	 * @returns Wire ref, or `undefined` when not a stream
	 */
	tryRegisterLocal(value: object): StreamRef | undefined {
		const mode = detectStreamMode(value);

		if (!mode) return undefined;

		for (const entry of this.entries.values()) {
			if (entry.local === value || entry.proxy === value) {
				return entry.ref;
			}
		}

		const ref = streamRef(
			this.owner,
			this.nextId++,
			mode,
			detectObjectMode(value, mode),
		);
		const entry: LocalStreamEntry = {
			closed: false,
			isOwner: true,
			local: value as Readable | Writable | Duplex,
			ref,
			writePending: new Map(),
		};

		this.entries.set(entryKey(ref.owner, ref.streamId), entry);
		this.attachOwnerListeners(entry);

		return ref;
	}

	/**
	 * Creates (or reuses) a local Node stream proxy for a remote {@link StreamRef}.
	 * @param ref - Wire tag from decode
	 * @returns Local Readable, Writable, or Duplex proxy
	 */
	createProxy(ref: StreamRef): Readable | Writable | Duplex {
		const key = entryKey(ref.owner, ref.streamId);
		const existing = this.entries.get(key);

		if (existing?.proxy) {
			return existing.proxy;
		}

		const writePending: WritePending = new Map();
		const entry: LocalStreamEntry = {
			closed: false,
			isOwner: false,
			local: undefined as unknown as Readable,
			ref,
			writePending,
		};

		this.entries.set(key, entry);

		const transport = this.createTransport(ref);
		let proxy: Readable | Writable | Duplex;

		if (ref.mode === "readable") {
			proxy = this.createReadableProxy(ref, transport);
		} else if (ref.mode === "writable") {
			proxy = this.createWritableProxy(ref, transport, writePending);
		} else {
			proxy = this.createDuplexProxy(ref, transport, writePending);
		}

		entry.local = proxy;
		entry.proxy = proxy;

		return proxy;
	}

	/**
	 * @param owner - Stream owner (`"host"` or worker id)
	 * @param streamId - Stream id within that owner
	 * @returns Whether this bridge tracks the given stream
	 */
	has(owner: StreamOwner, streamId: number): boolean {
		return this.entries.has(entryKey(owner, streamId));
	}

	/**
	 * Looks up an entry by owner + streamId.
	 * @param owner - Stream owner (`"host"` or worker id)
	 * @param streamId - Stream id within that owner
	 * @returns Entry, or `undefined` when not tracked
	 */
	private get(
		owner: StreamOwner,
		streamId: number,
	): LocalStreamEntry | undefined {
		return this.entries.get(entryKey(owner, streamId));
	}

	/**
	 * Forwards local owner stream events to the remote consumer via transport.
	 * @param entry - Newly registered owner-side stream
	 */
	private attachOwnerListeners(entry: LocalStreamEntry): void {
		const transport = this.createTransport(entry.ref);
		const { local, ref } = entry;

		if (ref.mode === "readable" || ref.mode === "duplex") {
			const readable = local as Readable;

			readable.on("data", (chunk: unknown) => {
				if (entry.closed) return;
				transport.sendData(chunk);
			});
			readable.on("end", () => {
				if (entry.closed) return;
				transport.sendEnd();
			});
			readable.on("error", (err: Error) => {
				if (entry.closed) return;
				transport.sendError(err);
			});
		}

		if (ref.mode === "writable" || ref.mode === "duplex") {
			const writable = local as Writable;

			writable.on("error", (err: Error) => {
				if (entry.closed) return;
				transport.sendError(err);
			});
		}
	}

	/**
	 * Builds a Readable proxy that pulls resume/pause across the wire.
	 * @param ref - Wire stream ref (objectMode, etc.)
	 * @param transport - Send hooks for this ref
	 * @returns Local Readable proxy
	 */
	private createReadableProxy(
		ref: StreamRef,
		transport: StreamTransport,
	): Readable {
		const readable = new Readable({
			objectMode: ref.objectMode,
			read: () => {
				transport.sendResume();
			},
		});

		readable.on("pause", () => {
			transport.sendPause();
		});
		readable.on("close", () => {
			transport.sendClose();
		});

		return readable;
	}

	/**
	 * Builds a Writable proxy; each write waits for a remote ack.
	 * @param ref - Wire stream ref (objectMode, etc.)
	 * @param transport - Send hooks for this ref
	 * @param pending - Map of write-id acknowledgements
	 * @returns Local Writable proxy
	 */
	private createWritableProxy(
		ref: StreamRef,
		transport: StreamTransport,
		pending: WritePending,
	): Writable {
		let nextWriteId = 1;

		const writable = new Writable({
			final: (cb) => {
				const id = nextWriteId++;

				pending.set(id, {
					reject: (err) => cb(err),
					resolve: () => cb(),
				});
				transport.sendWriteEnd(id);
			},
			objectMode: ref.objectMode,
			write: (chunk, _enc, cb) => {
				const id = nextWriteId++;

				pending.set(id, {
					reject: (err) => cb(err),
					resolve: () => cb(),
				});
				transport.sendWrite(id, chunk);
			},
		});

		writable.on("close", () => {
			transport.sendClose();
		});

		return writable;
	}

	/**
	 * Builds a Duplex proxy (readable + writable halves over the wire).
	 * @param ref - Wire stream ref (objectMode, etc.)
	 * @param transport - Send hooks for this ref
	 * @param pending - Map of write-id acknowledgements
	 * @returns Local Duplex proxy
	 */
	private createDuplexProxy(
		ref: StreamRef,
		transport: StreamTransport,
		pending: WritePending,
	): Duplex {
		let nextWriteId = 1;

		const duplex = new Duplex({
			final: (cb) => {
				const id = nextWriteId++;

				pending.set(id, {
					reject: (err) => cb(err),
					resolve: () => cb(),
				});
				transport.sendWriteEnd(id);
			},
			objectMode: ref.objectMode,
			read: () => {
				transport.sendResume();
			},
			write: (chunk, _enc, cb) => {
				const id = nextWriteId++;

				pending.set(id, {
					reject: (err) => cb(err),
					resolve: () => cb(),
				});
				transport.sendWrite(id, chunk);
			},
		});

		duplex.on("pause", () => {
			transport.sendPause();
		});
		duplex.on("close", () => {
			transport.sendClose();
		});

		return duplex;
	}

	/**
	 * Pushes a chunk into a local readable proxy (consumer side).
	 * Sends pause when the proxy buffer is full.
	 * @param owner - Stream owner (`"host"` or worker id)
	 * @param streamId - Stream id within that owner
	 * @param chunk - Decoded data chunk
	 */
	onRemoteData(owner: StreamOwner, streamId: number, chunk: unknown): void {
		const entry = this.get(owner, streamId);

		if (!entry || entry.closed || !entry.proxy) return;
		const readable = entry.proxy as Readable;

		if (typeof readable.push === "function") {
			const ok = readable.push(chunk);

			if (!ok) {
				this.createTransport(entry.ref).sendPause();
			}
		}
	}

	/**
	 * Signals EOF on a local readable proxy.
	 * @param owner - Stream owner (`"host"` or worker id)
	 * @param streamId - Stream id within that owner
	 */
	onRemoteEnd(owner: StreamOwner, streamId: number): void {
		const entry = this.get(owner, streamId);

		if (!entry || entry.closed || !entry.proxy) return;
		const readable = entry.proxy as Readable;

		if (typeof readable.push === "function") {
			readable.push(null);
		}
	}

	/**
	 * Destroys local/proxy streams after a remote error.
	 * @param owner - Stream owner (`"host"` or worker id)
	 * @param streamId - Stream id within that owner
	 * @param err - Error from the remote side
	 */
	onRemoteError(owner: StreamOwner, streamId: number, err: Error): void {
		const entry = this.get(owner, streamId);

		if (!entry || entry.closed) return;
		entry.closed = true;
		entry.local.destroy(err);
		if (entry.proxy && entry.proxy !== entry.local) {
			entry.proxy.destroy(err);
		}
	}

	/**
	 * Applies backpressure on the owner-side readable.
	 * @param owner - Stream owner (`"host"` or worker id)
	 * @param streamId - Stream id within that owner
	 */
	onPause(owner: StreamOwner, streamId: number): void {
		const entry = this.get(owner, streamId);

		if (!entry || !entry.isOwner) return;
		const readable = entry.local as Readable;

		if (typeof readable.pause === "function") {
			readable.pause();
		}
	}

	/**
	 * Resumes the owner-side readable after consumer drain.
	 * @param owner - Stream owner (`"host"` or worker id)
	 * @param streamId - Stream id within that owner
	 */
	onResume(owner: StreamOwner, streamId: number): void {
		const entry = this.get(owner, streamId);

		if (!entry || !entry.isOwner) return;
		const readable = entry.local as Readable;

		if (typeof readable.resume === "function") {
			readable.resume();
		}
	}

	/**
	 * Owner-side: writes a chunk from a remote writable proxy, then acks.
	 * @param owner - Stream owner (`"host"` or worker id)
	 * @param streamId - Stream id within that owner
	 * @param id - Correlation id for {@link onWriteResult}
	 * @param chunk - Chunk to write to the local writable
	 */
	async onWrite(
		owner: StreamOwner,
		streamId: number,
		id: number,
		chunk: unknown,
	): Promise<void> {
		const entry = this.get(owner, streamId);

		if (!entry || !entry.isOwner) {
			throw new Error(`Unknown stream ${owner}:${streamId}`);
		}

		const writable = entry.local as Writable;
		const transport = this.createTransport(entry.ref);

		try {
			await new Promise<void>((resolve, reject) => {
				writable.write(chunk as never, (err) => {
					if (err) reject(err);
					else resolve();
				});
			});
			transport.sendWriteResult(id);
		} catch (err) {
			transport.sendWriteResult(
				id,
				err instanceof Error ? err : new Error(String(err)),
			);
		}
	}

	/**
	 * Owner-side: ends the local writable after the remote proxy calls `end`.
	 * @param owner - Stream owner (`"host"` or worker id)
	 * @param streamId - Stream id within that owner
	 * @param id - Correlation id for {@link onWriteResult}
	 */
	async onWriteEnd(
		owner: StreamOwner,
		streamId: number,
		id: number,
	): Promise<void> {
		const entry = this.get(owner, streamId);

		if (!entry || !entry.isOwner) {
			throw new Error(`Unknown stream ${owner}:${streamId}`);
		}

		const writable = entry.local as Writable;
		const transport = this.createTransport(entry.ref);

		try {
			await new Promise<void>((resolve, reject) => {
				writable.end((err?: Error | null) => {
					if (err) reject(err);
					else resolve();
				});
			});
			transport.sendWriteResult(id);
		} catch (err) {
			transport.sendWriteResult(
				id,
				err instanceof Error ? err : new Error(String(err)),
			);
		}
	}

	/**
	 * Resolves/rejects a pending writable proxy write by correlation id.
	 * @param owner - Stream owner (`"host"` or worker id)
	 * @param streamId - Stream id within that owner
	 * @param id - Correlation id from the pending write
	 * @param error - Optional error when the remote write failed
	 */
	onWriteResult(
		owner: StreamOwner,
		streamId: number,
		id: number,
		error?: Error,
	): void {
		const entry = this.get(owner, streamId);
		const waiter = entry?.writePending.get(id);

		if (!waiter) return;
		entry?.writePending.delete(id);
		if (error) waiter.reject(error);
		else waiter.resolve();
	}

	/**
	 * Force-destroys a tracked stream and drops its entry.
	 * @param owner - Stream owner (`"host"` or worker id)
	 * @param streamId - Stream id within that owner
	 */
	onClose(owner: StreamOwner, streamId: number): void {
		const key = entryKey(owner, streamId);
		const entry = this.entries.get(key);

		if (!entry || entry.closed) return;
		entry.closed = true;
		entry.local.destroy();
		if (entry.proxy && entry.proxy !== entry.local) {
			entry.proxy.destroy();
		}
		this.entries.delete(key);
	}

	/** Closes every stream tracked by this bridge (runtime dispose). */
	closeAll(): void {
		for (const entry of [...this.entries.values()]) {
			this.onClose(entry.ref.owner, entry.ref.streamId);
		}
	}
}

/**
 * Reads objectMode flags from a Node stream for the given mode.
 * @param value - Local stream instance
 * @param mode - Detected readable / writable / duplex
 * @returns Whether the stream uses object mode for that side
 */
function detectObjectMode(value: object, mode: StreamRef["mode"]): boolean {
	const stream = value as {
		readableObjectMode?: boolean;
		writableObjectMode?: boolean;
	};

	if (mode === "readable") return Boolean(stream.readableObjectMode);
	if (mode === "writable") return Boolean(stream.writableObjectMode);

	return Boolean(stream.readableObjectMode || stream.writableObjectMode);
}

/**
 * Composite map key so host and worker stream ids do not collide.
 * @param owner - Stream owner (`"host"` or worker id)
 * @param streamId - Stream id within that owner
 * @returns Key of the form `owner:streamId`
 */
function entryKey(owner: StreamOwner, streamId: number): string {
	return `${owner}:${streamId}`;
}

/**
 * Detects whether a value is a Node.js stream and which mode it has.
 * @param value - Candidate during encode
 * @returns `"readable"` | `"writable"` | `"duplex"`, or `undefined`
 */
export function detectStreamMode(
	value: object,
): StreamRef["mode"] | undefined {
	const readable = isNodeReadable(value);
	const writable = isNodeWritable(value);

	if (readable && writable) return "duplex";
	if (readable) return "readable";
	if (writable) return "writable";

	return undefined;
}
