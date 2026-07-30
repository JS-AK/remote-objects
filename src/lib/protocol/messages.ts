/** Wire tag for an actor living on a worker. */
export type ActorRef = {
	type: "actor_ref";
	workerId: number;
	objectId: number;
};

/**
 * Cross-worker actor method call, routed through the parent {@link WorkerNode}.
 * Posted by a worker that holds a stub for an actor on another worker.
 */
export type BridgeCallMessage = {
	bridge: "call";
	id: number;
	targetWorkerId: number;
	objectId: number;
	method: string;
	args: unknown[];
};

/**
 * Request to invoke a callback owned by the host or another worker.
 * Posted by a worker that received a {@link CallbackRef} stub.
 */
export type BridgeCallbackInvokeMessage = {
	bridge: "callback_invoke";
	id: number;
	owner: "host" | number;
	callbackId: number;
	args: unknown[];
};

/** Reply to {@link BridgeCallbackInvokeMessage}. */
export type BridgeCallbackResultMessage =
	| {
		bridge: "callback_result";
		id: number;
		result: unknown;
	}
	| {
		bridge: "callback_result";
		id: number;
		error: {
			message: string;
			name?: string;
			stack?: string;
		};
	};

/** Reply to {@link BridgeCallMessage}. */
export type BridgeResultMessage =
	| {
		bridge: "result";
		id: number;
		result: unknown;
	}
	| {
		bridge: "result";
		id: number;
		error: {
			message: string;
			name?: string;
			stack?: string;
		};
	};

/**
 * Stream control / data messages for {@link StreamRef} bridges.
 * `direction` distinguishes owner→consumer (`from_owner`) vs consumer→owner (`to_owner`).
 */
export type BridgeStreamMessage =
	| {
		bridge: "stream_data";
		owner: "host" | number;
		streamId: number;
		chunk: unknown;
		direction: "to_owner" | "from_owner";
	}
	| {
		bridge: "stream_end";
		owner: "host" | number;
		streamId: number;
		direction: "to_owner" | "from_owner";
	}
	| {
		bridge: "stream_error";
		owner: "host" | number;
		streamId: number;
		direction: "to_owner" | "from_owner";
		error: {
			message: string;
			name?: string;
			stack?: string;
		};
	}
	| {
		bridge: "stream_pause";
		owner: "host" | number;
		streamId: number;
	}
	| {
		bridge: "stream_resume";
		owner: "host" | number;
		streamId: number;
	}
	| {
		bridge: "stream_write_ack";
		owner: "host" | number;
		streamId: number;
		id: number;
	}
	| {
		bridge: "stream_write";
		owner: "host" | number;
		streamId: number;
		id: number;
		chunk: unknown;
	}
	| {
		bridge: "stream_write_end";
		owner: "host" | number;
		streamId: number;
		id: number;
	}
	| {
		bridge: "stream_write_result";
		owner: "host" | number;
		streamId: number;
		id: number;
		error?: {
			message: string;
			name?: string;
			stack?: string;
		};
	}
	| {
		bridge: "stream_close";
		owner: "host" | number;
		streamId: number;
	};

/** Wire tag for a callable living on the host or a worker. */
export type CallbackRef = {
	type: "callback_ref";
	owner: "host" | number;
	callbackId: number;
};

/**
 * Host → worker command messages (register / create / call / destroy / callbacks).
 * Each carries a correlation `id` answered by {@link ProtocolResponse}.
 */
export type ProtocolMessage =
	| {
		command: "register";
		id: number;
		className: string;
		moduleUrl: string;
		exportName: string;
	}
	| {
		command: "create";
		id: number;
		objectId: number;
		className: string;
		args: unknown[];
	}
	| {
		command: "call";
		id: number;
		objectId: number;
		method: string;
		args: unknown[];
	}
	| {
		command: "destroy";
		id: number;
		objectId: number;
		close?: boolean;
	}
	| {
		command: "close_all";
		id: number;
	}
	| {
		command: "callback_invoke";
		id: number;
		callbackId: number;
		args: unknown[];
	}
	| {
		command: "callback_release";
		id: number;
		callbackIds: number[];
	};

/** Worker → host reply to a {@link ProtocolMessage}. */
export type ProtocolResponse =
	| {
		id: number;
		result: unknown;
	}
	| {
		id: number;
		error: {
			message: string;
			name?: string;
			stack?: string;
		};
	};

/** Wire tag for a Node.js stream living on the host or a worker. */
export type StreamRef = {
	type: "stream_ref";
	owner: "host" | number;
	streamId: number;
	mode: "readable" | "writable" | "duplex";
	objectMode: boolean;
};

/** Structured-clone-safe error payload for wire messages. */
export type WireError = {
	message: string;
	name?: string;
	stack?: string;
};

/**
 * Rebuilds an `Error` from a wire payload (preserves name/stack when present).
 * @param error - Serialized error fields
 * @returns Reconstructed Error instance
 */
export function fromWireError(error: WireError): Error {
	const err = new Error(error.message);

	if (error.name) err.name = error.name;
	if (error.stack) err.stack = error.stack;

	return err;
}

/**
 * Serializes any thrown value into a {@link WireError} for postMessage.
 * @param err - Caught error or arbitrary throw value
 * @returns Structured-clone-safe error payload
 */
export function toWireError(err: unknown): WireError {
	const error = err instanceof Error ? err : new Error(String(err));

	return {
		message: error.message,
		name: error.name,
		...(error.stack ? { stack: error.stack } : {}),
	};
}
