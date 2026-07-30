/* eslint-disable sort-imports */
import type { BridgeStreamMessage, StreamRef } from "../protocol/messages.js";
import { toWireError } from "../protocol/messages.js";
import type { StreamTransport } from "../protocol/stream-bridge.js";
import type { StreamRouter } from "./stream-router.js";
import type { WorkerNode } from "./worker-node.js";

/**
 * Builds host-side {@link StreamTransport} hooks for a single {@link StreamRef}.
 * Routes owner events to subscribed workers and consumer control messages to the owner.
 *
 * @param getWorkers - Snapshot accessor for the runtime's worker pool
 * @param streamRouter - Shared subscription table
 * @param ref - Stream being transported
 * @returns Transport callbacks used by {@link StreamBridge}
 */
export function createHostStreamTransport(
	getWorkers: () => WorkerNode[],
	streamRouter: StreamRouter,
	ref: StreamRef,
): StreamTransport {
	const postToConsumers = (msg: BridgeStreamMessage) => {
		for (const consumer of streamRouter.consumers(ref.owner, ref.streamId)) {
			if (consumer === "host") continue;
			getWorkers()[consumer as number]?.postToWorker(msg);
		}
	};

	const postToOwner = (msg: BridgeStreamMessage) => {
		if (ref.owner === "host") return;
		getWorkers()[ref.owner]?.postToWorker(msg);
	};

	return {
		sendClose: () => {
			postToOwner({
				bridge: "stream_close",
				owner: ref.owner,
				streamId: ref.streamId,
			});
		},
		sendData: (chunk) => {
			postToConsumers({
				bridge: "stream_data",
				chunk,
				direction: "from_owner",
				owner: ref.owner,
				streamId: ref.streamId,
			});
		},
		sendEnd: () => {
			postToConsumers({
				bridge: "stream_end",
				direction: "from_owner",
				owner: ref.owner,
				streamId: ref.streamId,
			});
		},
		sendError: (err) => {
			postToConsumers({
				bridge: "stream_error",
				direction: "from_owner",
				error: toWireError(err),
				owner: ref.owner,
				streamId: ref.streamId,
			});
		},
		sendPause: () => {
			postToOwner({
				bridge: "stream_pause",
				owner: ref.owner,
				streamId: ref.streamId,
			});
		},
		sendResume: () => {
			postToOwner({
				bridge: "stream_resume",
				owner: ref.owner,
				streamId: ref.streamId,
			});
		},
		sendWrite: (id, chunk) => {
			postToOwner({
				bridge: "stream_write",
				chunk,
				id,
				owner: ref.owner,
				streamId: ref.streamId,
			});
		},
		sendWriteEnd: (id) => {
			postToOwner({
				bridge: "stream_write_end",
				id,
				owner: ref.owner,
				streamId: ref.streamId,
			});
		},
		sendWriteResult: (id, error) => {
			postToConsumers({
				bridge: "stream_write_result",
				id,
				owner: ref.owner,
				streamId: ref.streamId,
				...(error ? { error: toWireError(error) } : {}),
			});
		},
	};
}
