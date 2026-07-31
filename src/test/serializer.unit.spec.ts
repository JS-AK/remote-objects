import {
	describe, expect, it,
} from "vitest";

import { Serializer } from "../lib/protocol/serializer.js";

describe("Serializer", () => {
	it("encodes shared (non-circular) object references in a DAG", () => {
		const s = new Serializer();
		const buyData = {
			avgPrice: 100,
			location: "x" as const,
			maxPrice: 100,
			minPrice: 100,
			ordersCount: 1,
			xOrdersCount: 1,
		};
		const sellData = {
			avgPrice: 110,
			location: "x" as const,
			maxPrice: 110,
			minPrice: 110,
			ordersCount: 1,
			xOrdersCount: 1,
		};

		// Nested summary + top-level fields point at the same objects.
		const result = {
			items: [{
				arbitrage: {
					buy: buyData,
					profit: 1,
					sell: sellData,
				},
				buy: buyData,
				sell: sellData,
				typeID: 1,
			}],
			staleSpread: [],
		};

		expect(() => s.encode(result)).not.toThrow();
	});

	it("rejects true circular references", () => {
		const s = new Serializer();
		const circular: Record<string, unknown> = { value: 1 };
		
		circular.self = circular;

		expect(() => s.encode(circular)).toThrow(
			/Circular references are not supported/,
		);
	});
});
