# @js-ak/remote-objects

Actor-style remote objects on Node.js worker threads — write normal classes, call them like local instances.

```ts
import { Runtime, actor } from "@js-ak/remote-objects";

export class Counter {
  constructor(public value = 0) {}
  inc() {
    this.value += 1;
    return this.value;
  }
}
actor(Counter, import.meta);

const runtime = new Runtime({ workers: 2 });
const counter = await runtime.spawn(Counter, 10);
console.log(await counter.inc()); // 11
await runtime.dispose();
```

## Install

```bash
npm install @js-ak/remote-objects
```

Requires Node.js 20.19+. Works with both ESM and CommonJS:

```ts
import { Runtime, actor } from "@js-ak/remote-objects"; // ESM
```

```js
const { Runtime, actor } = require("@js-ak/remote-objects"); // CJS
```

## Core ideas

- **`runtime.spawn(Class, ...args)`** creates an actor on a worker and returns a typed proxy
- **Methods are always async** from the caller’s side (even if the class method is sync)
- **Actors are sticky** — an instance stays on one worker until `destroy` / `dispose`
- **`return this`** becomes an actor reference (same proxy identity), not a cloned object
- **Callbacks and Node.js streams** can cross the boundary as args/results
- **Workers are an implementation detail** — the public API is objects and methods

## Binding classes

Each actor class must be bound in its own module so workers know which file to load.

ESM:

```ts
import { actor } from "@js-ak/remote-objects";

export class Database { /* ... */ }
actor(Database, import.meta);
```

CJS:

```js
const { actor } = require("@js-ak/remote-objects");

class Database { /* ... */ }
actor(Database, __filename);
// or: actor(Database, { filename: __filename })
module.exports = { Database };
```

`spawn` auto-registers the class on first use (or call `runtime.register(Database)` explicitly).

## Options

```ts
new Runtime({
  workers: 4,
  debug: true, // or (event) => { ... } or { onEvent: (event) => { ... } }
  callTimeoutMs: 5_000,
});
```

Debug events include `register`, `spawn`, `destroy`, `call:start`, `call:end`, `call:timeout`, `worker:error`, `bridge:call`, `bridge:result`, `dispose`. Spawn/call events carry `actorId` as `"workerId:objectId"`.

## Lifecycle

```ts
await runtime.destroy(counter); // close (dispose/close) then drop one actor
await runtime.destroy(counter, { close: false }); // drop without close
await runtime.dispose();        // close actors, drain, terminate workers
await runtime.dispose({ closeActors: false });
```

After `dispose`, further `spawn` / method calls fail with a clear error.

## Passing actors as arguments

Proxies can be passed into methods (including across workers), including nested inside plain objects/arrays:

```ts
await linker.link(counter);
await linker.readOther();

const wrapped = await nested.wrap(counter); // { counter, label }
await nested.readWrapped(wrapped);
```

## Callbacks

Functions may be passed as arguments or returned from methods. Remote invocations are always async.

```ts
await actor.withProgress(10, async (n) => {
  console.log("progress", n);
});

const add = await actor.makeAdder(10);
await add(5); // 15
```

- Callbacks in **args** live until that method call finishes (plus in-flight invokes)
- Callbacks in **return values** live until the owning actor is destroyed
- Errors inside callbacks reject the remote invoke

## Streams

Node.js `Readable` / `Writable` / `Duplex` can be args or results (objectMode preserved; backpressure via pause/resume).

```ts
const stream = await actor.query(100);
for await (const row of stream) {
  // ...
}
```

## Isolation patterns

Use sticky actors when a worker should own long-lived state (DB pools, SDK clients, caches):

1. Put the client behind an actor class; bind with `actor(Class, import.meta)`
2. Prefer method calls over sharing mutable state across threads
3. Use callbacks for progress / notifications; streams for row batches or ingest
4. Call `destroy(proxy)` (default `close: true`) or `dispose()` so `close`/`dispose` on the actor runs

Same-actor calls are serialized (mailbox). Different actors may run in parallel on the pool.

## Compared to similar tools

| | remote-objects | Comlink | Piscina |
|--|----------------|---------|---------|
| Model | Sticky class actors + proxies | RPC proxies | Task pool |
| Best for | Stateful isolation (DB/SDK) | General worker RPC | Stateless jobs |
| Callbacks / streams | Yes (Node streams) | Callbacks / proxies | Per-task message |
| Migration between workers | No | N/A | N/A |

## Helpers

```ts
import { getActorHandle } from "@js-ak/remote-objects";

const handle = getActorHandle(counter); // { workerId, objectId } | undefined
```

## Limits

- Arguments and results must be structured-clone compatible (plus actor / callback / stream refs)
- Deep encoding walks arrays and plain objects only (not arbitrary class instances)
- You cannot read instance fields through the proxy — only call methods
- Overlapping calls to the **same** actor are queued (mailbox); different actors may run in parallel
- Actors are not migrated between workers; identity is fixed at spawn
- Circular structures in encoded plain objects are rejected with a clear error

## License

MIT
