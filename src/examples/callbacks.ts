/* eslint-disable no-console */
import { Runtime } from "../index.js";
import { Streamer } from "./actors/streamer.js";

const runtime = new Runtime({ workers: 1 });
const streamer = await runtime.spawn(Streamer);

try {
	const rows: Array<{ id: number; value: number; }> = [];

	const n = await streamer.forEachRow(5, async (row) => {
		rows.push(row);
	});

	console.log("rows processed:", n, rows);
} finally {
	await runtime.dispose();
}
