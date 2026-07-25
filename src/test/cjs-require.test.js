import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { describe, it, after } from "node:test";

const require = createRequire(import.meta.url);
const { Runtime } = require("../../build/cjs/index.js");
const CjsCounter = require("./fixtures/cjs-counter.cjs");

describe("cjs dual package", () => {
  /** @type {InstanceType<typeof Runtime>[]} */
  const runtimes = [];

  after(async () => {
    await Promise.all(runtimes.map((r) => r.dispose({ closeActors: false })));
  });

  it("loads via require()", () => {
    assert.equal(typeof Runtime, "function");
    assert.equal(typeof require("../../build/cjs/index.js").actor, "function");
  });

  it("resolves package exports for require()", () => {
    const pkg = require("../..");
    assert.equal(typeof pkg.Runtime, "function");
    assert.equal(typeof pkg.actor, "function");
  });

  it("spawns a CJS actor bound with __filename", async () => {
    const runtime = new Runtime({ workers: 1 });
    runtimes.push(runtime);

    const counter = await runtime.spawn(CjsCounter, 5);
    assert.equal(await counter.inc(), 6);
    assert.equal(await counter.getValue(), 6);
  });
});
