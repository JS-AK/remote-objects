import {
	describe, expect, it,
} from "vitest";

import { CallbackRegistry } from "../lib/protocol/callback-registry.js";

describe("CallbackRegistry", () => {
	it("releaseBoundToObject drops only callbacks bound to that actor", () => {
		const registry = new CallbackRegistry("host");
		const keep = registry.register(() => undefined);
		const dropA1 = registry.register(() => undefined, { boundObjectId: 1 });
		const dropA2 = registry.register(() => undefined, { boundObjectId: 1 });
		const dropB = registry.register(() => undefined, { boundObjectId: 2 });

		registry.releaseBoundToObject(1);

		expect(registry.get(keep.callbackId)).toBeDefined();
		expect(registry.get(dropA1.callbackId)).toBeUndefined();
		expect(registry.get(dropA2.callbackId)).toBeUndefined();
		expect(registry.get(dropB.callbackId)).toBeDefined();
	});

	it("release and releaseCallScoped keep the bound index consistent", () => {
		const registry = new CallbackRegistry("host");
		const scoped = registry.register(() => undefined, {
			boundObjectId: 7,
			callScoped: true,
		});
		const bound = registry.register(() => undefined, { boundObjectId: 7 });

		registry.releaseCallScoped([scoped.callbackId]);
		expect(registry.get(bound.callbackId)).toBeDefined();

		registry.release([bound.callbackId]);
		registry.releaseBoundToObject(7);
		expect(registry.get(bound.callbackId)).toBeUndefined();
	});

	it("clear removes every entry", () => {
		const registry = new CallbackRegistry(0);
		const a = registry.register(() => undefined, { boundObjectId: 3 });
		const b = registry.register(() => undefined);

		registry.clear();

		expect(registry.get(a.callbackId)).toBeUndefined();
		expect(registry.get(b.callbackId)).toBeUndefined();
		registry.releaseBoundToObject(3);
		expect(registry.get(b.callbackId)).toBeUndefined();
	});
});
