import { PG } from "@js-ak/db-manager";

import { actor } from "../../index.js";

export type DbCreds = {
	database: string;
	host: string;
	password: string;
	port: number;
	user: string;
};

/**
 * DB actor: pool lives inside the worker that owns this instance.
 * All queries for this proxy stick to that worker.
 */
export class Database {
	private readonly creds: DbCreds;
	private readonly poolName: string;
	private readonly pool: ReturnType<typeof PG.connection.getStandardPool>;

	constructor(creds: DbCreds, poolName = `db-${Math.random().toString(36).slice(2)}`) {
		this.creds = creds;
		this.poolName = poolName;
		this.pool = PG.connection.getStandardPool(creds, poolName);
	}

	async query<T extends Record<string, unknown> = Record<string, unknown>>(
		sql: string,
		params: unknown[] = [],
	): Promise<T[]> {
		const result = await this.pool.query<T>(sql, params);

		return result.rows;
	}

	async queryOne<T extends Record<string, unknown> = Record<string, unknown>>(
		sql: string,
		params: unknown[] = [],
	): Promise<T | null> {
		const rows = await this.query<T>(sql, params);

		return rows[0] ?? null;
	}

	async close(): Promise<void> {
		await PG.connection.removeStandardPool(this.creds, this.poolName);
	}
}

actor(Database, import.meta);
