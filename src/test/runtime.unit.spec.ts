/* eslint-disable sort-imports */
import { Readable } from "node:stream";

import {
	afterAll,
	describe,
	expect,
	it,
} from "vitest";

import { Runtime, getActorHandle } from "../index.js";
import { CallbackActor } from "./fixtures/callback-actor.js";
import { Closable, ClosableProbe } from "./fixtures/closable.js";
import { Counter } from "./fixtures/counter.js";
import { Linker } from "./fixtures/linker.js";
import { MailboxActor } from "./fixtures/mailbox-actor.js";
import { NestedActors } from "./fixtures/nested-actors.js";
import { SlowActor } from "./fixtures/slow-actor.js";
import { StreamActor } from "./fixtures/stream-actor.js";

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

	it("closes actor on destroy by default", async () => {
		const runtime = track(new Runtime({ workers: 1 }));
		const probe = await runtime.spawn(ClosableProbe);

		await probe.reset();
		const actor = await runtime.spawn(Closable);

		await runtime.destroy(actor);
		expect(await probe.wasLastClosed()).toBe(true);
	});

	it("skips close on destroy when close:false", async () => {
		const runtime = track(new Runtime({ workers: 1 }));
		const probe = await runtime.spawn(ClosableProbe);

		await probe.reset();
		const actor = await runtime.spawn(Closable);

		await runtime.destroy(actor, { close: false });
		expect(await probe.wasLastClosed()).toBe(false);
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
		const events: string[] = [];

		const runtime2 = track(new Runtime({
			callTimeoutMs: 50,
			debug: (e) => events.push(e.type),
			workers: 1,
		}));
		const slow2 = await runtime2.spawn(SlowActor);

		await expect(slow.wait(200)).rejects.toThrow(/timed out/);
		await expect(slow2.wait(200)).rejects.toThrow(/timed out/);
		expect(events).toContain("call:timeout");
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

	it("deep-encodes nested actor refs in plain objects", async () => {
		const runtime = track(new Runtime({ workers: 1 }));
		const counter = await runtime.spawn(Counter, 3);
		const nested = await runtime.spawn(NestedActors);
		const wrapped = await nested.wrap(counter);

		expect(wrapped.label).toBe("wrapped");
		expect(await nested.readWrapped(wrapped)).toBe(3);
		expect(await wrapped.counter.getValue()).toBe(3);
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

	it("invokes progress callbacks passed as arguments", async () => {
		const runtime = track(new Runtime({ workers: 1 }));
		const actor = await runtime.spawn(CallbackActor);
		const seen: number[] = [];

		const sum = await actor.withProgress(4, async (v) => {
			seen.push(v);
		});

		expect(sum).toBe(10);
		expect(seen).toEqual([1, 2, 3, 4]);
	});

	it("propagates errors from callbacks", async () => {
		const runtime = track(new Runtime({ workers: 1 }));
		const actor = await runtime.spawn(CallbackActor);

		await expect(actor.boom(async () => {
			throw new Error("cb fail");
		})).rejects.toThrow(/cb fail/);
	});

	it("returns callable callbacks from actors", async () => {
		const runtime = track(new Runtime({ workers: 1 }));
		const actor = await runtime.spawn(CallbackActor);
		const add = await actor.makeAdder(10);

		expect(await add(5)).toBe(15);
	});

	it("invokes callbacks across workers", async () => {
		const runtime = track(new Runtime({ workers: 2 }));
		const actor = await runtime.spawn(CallbackActor); // worker 0
		const linker = await runtime.spawn(Linker); // worker 1 — just to use 2 workers

		void linker;

		const seen: number[] = [];
		const sum = await actor.withProgress(3, (v) => {
			seen.push(v);
		});

		expect(sum).toBe(6);
		expect(seen).toEqual([1, 2, 3]);
	});

	it("streams readable results from actors", async () => {
		const runtime = track(new Runtime({ workers: 1 }));
		const actor = await runtime.spawn(StreamActor);
		const stream = await actor.numbers(5);
		const items: unknown[] = [];

		for await (const chunk of stream as Readable) {
			items.push(chunk);
		}

		expect(items).toEqual([0, 1, 2, 3, 4]);
	});

	it("accepts readable streams as arguments", async () => {
		const runtime = track(new Runtime({ workers: 1 }));
		const actor = await runtime.spawn(StreamActor);
		const input = Readable.from([10, 20, 30], { objectMode: true });
		const items = await actor.collect(input);

		expect(items).toEqual([10, 20, 30]);
	});

	it("accepts writable streams as nested return values", async () => {
		const runtime = track(new Runtime({ workers: 1 }));
		const actor = await runtime.spawn(StreamActor);
		const { done, stream } = await actor.sink();

		stream.write(1);
		stream.write(2);
		stream.end();
		expect(await done()).toEqual([1, 2]);
	});

	it("rejects circular structures with a clear error", async () => {
		const runtime = track(new Runtime({ workers: 1 }));
		const counter = await runtime.spawn(Counter, 0);
		const circular: { self?: unknown; } = {};

		circular.self = circular;

		await expect(
			(counter as unknown as { add: (n: unknown) => Promise<number>; }).add(
				circular,
			),
		).rejects.toThrow(/Circular references/);
	});
});
