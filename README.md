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
- **`runtime.getOrSpawn(key, Class, ...args)`** returns one actor per key (same proxy until `destroy`); placement is `hash(key) % workers`
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

`spawn` and `getOrSpawn` auto-register the class on first use (or call `runtime.register(Database)` explicitly).

## Options

```ts
new Runtime({
  workers: 4,
  debug: true, // or (event) => { ... } or { onEvent: (event) => { ... } }
  callTimeoutMs: 5_000,
});
```

Debug events include `register`, `spawn`, `destroy`, `call:start`, `call:end`, `call:timeout`, `worker:error`, `bridge:call`, `bridge:result`, `dispose`. Spawn/call events carry `actorId` as `"workerId:objectId"`. **`getOrSpawn` emits the same `spawn` event** when it creates a new actor (cache hits do not spawn again).

## Lifecycle

```ts
await runtime.destroy(counter); // close (dispose/close) then drop one actor
await runtime.destroy(counter, { close: false }); // drop without close
await runtime.dispose();        // close actors, drain, terminate workers
await runtime.dispose({ closeActors: false });
```

After `dispose`, further `spawn`, `getOrSpawn`, or method calls fail with a clear error.

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

## When to use what

You do not need to predict every app up front — pick a pattern from what you are doing:

| You want… | Use |
|-----------|-----|
| Long-lived client state (DB pool, SDK session, in-memory cache) | One actor per resource; or **`getOrSpawn(key, ...)`** for one actor per tenant/shard |
| More throughput on CPU or I/O | `workers: 2+` and **separate** actor instances (each spawn → least-loaded worker) |
| Strict ordering for one object | One actor — overlapping calls on the same proxy are queued (mailbox) |
| Parallel work on the same class | Multiple `spawn`s, or `getOrSpawn` + extra `spawn`s (see `examples/db.ts`) |
| Progress / one-off handlers during a call | Callback in **args** (released when the call finishes) |
| Long-lived handler returned from a method | Callback in **return value** (released on `destroy` of the owning actor) |
| Many rows or chunked I/O | Streams as args or results (backpressure built in) |
| Compose actors (even on different workers) | Pass actor proxies as method arguments |
| Shut down one resource | `destroy(proxy)` — runs `close`/`dispose` on the actor by default |
| Shut down the whole runtime | `dispose()` — drain in-flight work, then terminate workers |

**Worker count**

- **`workers: 1`** — simplest mental model; all actors share one thread. Good for getting started or when isolation from the main thread is enough.
- **`workers: 2+`** — use when you want parallel actors on separate threads. Each new spawn is placed on the worker with the fewest live actors and in-flight requests; after that the actor stays sticky on that worker.

**One actor vs many**

- **One proxy, many calls** — state and side effects stay in one place; calls do not overlap on that instance.
- **Many proxies, same class** — independent state and true parallelism (e.g. two query actors, two connection pools).

**One actor per tenant / key**

Use **`getOrSpawn(key, Class, ...args)`** — the runtime keeps one proxy per key until `destroy`. The worker is chosen by `hash(key) % workers` and stays stable for that key. Reusing a key with a different class or constructor args throws.

```ts
const db = await runtime.getOrSpawn(`tenant:${tenantId}`, Database, creds);
// later, same process → same proxy / same pool
const same = await runtime.getOrSpawn(`tenant:${tenantId}`, Database, creds);
expect(same).toBe(db);

await runtime.destroy(db); // drops the key; next getOrSpawn creates a fresh actor
```

For **many independent instances** of the same class (parallel pools), use **`spawn`** instead. **`spawn` does not register a key** — `spawn(Database, creds)` and `getOrSpawn("db", Database, creds)` are always separate actors.

**Not a fit**

- Fire-and-forget stateless jobs on a pile of plain data → a task pool (e.g. Piscina) may be simpler.
- Shared mutable state on the main thread with no isolation → you do not need actors at all.
- Moving an existing actor to another worker after spawn → not supported; spawn again if you need a new placement.

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
- **`getOrSpawn` keys are per-runtime (in-memory)** — not shared across processes or runtimes
- **`spawn` and `getOrSpawn` use separate registries** — a keyed name does not attach to an actor created with `spawn`
- **Key placement is `hash(key) % workers`** — changing the worker pool size can move a key to a different worker on the next create (after `destroy`)
- **Repeated `getOrSpawn` compares constructor args** using deep equality for primitives, arrays, and plain objects only (not `Date`, `Map`, class instances, etc.)

## License

MIT
