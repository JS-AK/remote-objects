/* eslint-disable no-console */

import { Runtime } from "../index.js";
import { Streamer } from "./actors/streamer.js";

const runtime = new Runtime({ workers: 1 });
const streamer = await runtime.spawn(Streamer);

try {
	const stream = await streamer.query(5);

	for await (const row of stream) {
		console.log("stream row", row);
	}
} finally {
	await runtime.dispose();
}
