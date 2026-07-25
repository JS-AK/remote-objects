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

const db = await runtime.spawn(Database, creds);

try {
	const one = await db.queryOne<{ n: number; }>("SELECT 1::int AS n");

	console.log("select 1 ->", one);

	const version = await db.queryOne<{ version: string; }>("SELECT version()");

	console.log("version ->", version?.version);

	// two actors → round-robin across workers, each with its own pool
	const db2 = await runtime.spawn(Database, creds);
	const [a, b] = await Promise.all([
		db.queryOne("SELECT 42::int AS worker_probe"),
		db2.queryOne("SELECT 7::int AS worker_probe"),
	]);

	console.log("parallel ->", { a, b });

	await db2.close();
} finally {
	await db.close();
	await runtime.dispose();
}
