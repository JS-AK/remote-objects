import { defineConfig } from "vitest/config";

export default defineConfig({
	test: {
		environment: "node",
		// Runtime/worker tests need compiled ESM (worker loads build/esm + actor file: URLs).
		include: ["build/esm/**/*.unit.spec.js"],
		setupFiles: ["./vitest.setup.js"],
	},
});
