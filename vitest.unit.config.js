import { defineConfig } from "vitest/config";

export default defineConfig({
	test: {
		environment: "node",
		// Runtime/worker tests need compiled ESM (worker loads build/esm + actor file: URLs).
		include: ["build/esm/**/*.unit.spec.js"],
		setupFiles: ["./vitest.setup.js"],
		coverage: {
			provider: "v8",
			include: ["build/esm/lib/**/*.js"],
			exclude: [
				"build/esm/lib/worker/**",
				"build/esm/lib/index.js",
			],
			reporter: ["text", "lcov"],
			thresholds: {
				lines: 65,
				functions: 75,
				branches: 50,
				statements: 65,
			},
		},
	},
});
