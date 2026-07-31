import { Database, type DbCreds } from "./actors/database.js";
import { Runtime } from "../index.js";

const creds: DbCreds = {
	database: process.env.PG_DATABASE ?? "postgres",
	host: process.env.PG_HOST ?? "localhost",
	password: process.env.PG_PASSWORD ?? "admin",
	port: Number(process.env.PG_PORT ?? 5432),
	user: process.env.PG_USER ?? "postgres",
};

const runtime = new Runtime({ debug: true, workers: 2 });

// one pool for the app — reuse via getOrSpawn("main") from anywhere in-process
const db = await runtime.getOrSpawn("main", Database, creds);

try {
	const one = await db.queryOne<{ n: number; }>("SELECT 1::int AS n");

	console.log("select 1 ->", one);

	const version = await db.queryOne<{ version: string; }>("SELECT version()");

	console.log("version ->", version?.version);

	// second spawn → separate pool for parallel queries (load-balanced worker)
	const db2 = await runtime.spawn(Database, creds);
	const [a, b] = await Promise.all([
		db.queryOne("SELECT 42::int AS worker_probe"),
		db2.queryOne("SELECT 7::int AS worker_probe"),
	]);

	console.log("parallel ->", { a, b });

	// same key → same proxy as db (spawn above does not register "main")
	const same = await runtime.getOrSpawn("main", Database, creds);

	console.log("getOrSpawn reuse ->", same === db);

	await db2.close();
} finally {
	await db.close();
	await runtime.dispose();
}
