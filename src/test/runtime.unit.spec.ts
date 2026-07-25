import {
	afterAll,
	describe,
	expect,
	it,
} from "vitest";

import { Runtime, getActorHandle } from "../index.js";
import { Closable } from "./fixtures/closable.js";
import { Counter } from "./fixtures/counter.js";
import { Linker } from "./fixtures/linker.js";
import { MailboxActor } from "./fixtures/mailbox-actor.js";
import { SlowActor } from "./fixtures/slow-actor.js";

describe("remote-objects", () => {
	const runtimes: Runtime[] = [];

	function track(runtime: Runtime): Runtime {
		runtimes.push(runtime);

		return runtime;
	}

	afterAll(async () => {
		await Promise.all(runtimes.map((r) => r.dispose({ closeActors: false })));
	});

	it("spawns and calls methods", async () => {
		const runtime = track(new Runtime({ workers: 1 }));
		const counter = await runtime.spawn(Counter, 10);

		expect(await counter.inc()).toBe(11);
		expect(await counter.add(4)).toBe(15);
		expect(await counter.getValue()).toBe(15);
	});

	it("keeps actors sticky and round-robins spawn", async () => {
		const runtime = track(new Runtime({ workers: 2 }));
		const a = await runtime.spawn(Counter, 0);
		const b = await runtime.spawn(Counter, 0);
		const ha = getActorHandle(a);
		const hb = getActorHandle(b);

		expect(ha).toBeTruthy();
		expect(hb).toBeTruthy();
		expect(ha?.workerId).toBe(0);
		expect(hb?.workerId).toBe(1);
		expect(`${ha?.workerId}:${ha?.objectId}`).toBe("0:1");
		expect(`${hb?.workerId}:${hb?.objectId}`).toBe("1:1");

		await a.inc();
		await b.inc();
		expect(getActorHandle(a)?.workerId).toBe(0);
		expect(getActorHandle(b)?.workerId).toBe(1);
	});

	it("returns proxy for return this", async () => {
		const runtime = track(new Runtime({ workers: 1 }));
		const counter = await runtime.spawn(Counter, 0);
		const same = await counter.chain();

		expect(getActorHandle(same)?.objectId).toBe(getActorHandle(counter)?.objectId);
		expect(await same.getValue()).toBe(1);
	});

	it("propagates worker errors with stack", async () => {
		const runtime = track(new Runtime({ workers: 1 }));
		const counter = await runtime.spawn(Counter, 0);

		await expect((counter as { fail: () => Promise<never>; }).fail())
			.rejects
			.toSatisfy((err: Error) => {
				expect(err.message).toBe("boom");
				expect(String(err.stack)).toMatch(/fail|boom|counter/);

				return true;
			});
	});

	it("destroys actors and rejects later calls", async () => {
		const runtime = track(new Runtime({ workers: 1 }));
		const counter = await runtime.spawn(Counter, 0);

		await runtime.destroy(counter);
		await expect(counter.inc()).rejects.toThrow(/Unknown object/);
	});

	it("rejects work after dispose", async () => {
		const runtime = new Runtime({ workers: 1 });
		const counter = await runtime.spawn(Counter, 0);

		await runtime.dispose();
		await expect(counter.inc()).rejects.toThrow(/disposed/);
		await expect(runtime.spawn(Counter, 0)).rejects.toThrow(/disposed/);
	});

	it("gracefully closes actors on dispose", async () => {
		const runtime = new Runtime({ workers: 1 });
		const actor = await runtime.spawn(Closable);

		expect(await actor.isClosed()).toBe(false);
		await runtime.dispose();
		// Worker is gone; we only assert dispose completed without hanging.
	});

	it("times out slow calls", async () => {
		const runtime = track(new Runtime({ callTimeoutMs: 50, workers: 1 }));
		const slow = await runtime.spawn(SlowActor);

		await expect(slow.wait(200)).rejects.toThrow(/timed out/);
	});

	it("passes actor refs as method arguments", async () => {
		const runtime = track(new Runtime({ workers: 1 }));
		const counter = await runtime.spawn(Counter, 42);
		const linker = await runtime.spawn(Linker);

		await linker.link(counter);
		expect(await linker.readOther()).toBe(42);
	});

	it("passes actor refs across workers via bridge", async () => {
		const runtime = track(new Runtime({ workers: 2 }));
		const counter = await runtime.spawn(Counter, 7); // worker 0
		const linker = await runtime.spawn(Linker); // worker 1

		await linker.link(counter);
		expect(await linker.readOther()).toBe(7);
	});

	it("serializes calls per actor mailbox", async () => {
		const runtime = track(new Runtime({ workers: 1 }));
		const actor = await runtime.spawn(MailboxActor);
		const results = await Promise.all([
			actor.work(40),
			actor.work(40),
			actor.work(40),
		]);

		expect(results).toEqual([1, 2, 3]);
	});
});
