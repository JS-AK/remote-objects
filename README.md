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

Debug events include `register`, `spawn`, `destroy`, `call:start`, `call:end`, `dispose`. Spawn/call events carry `actorId` as `"workerId:objectId"`.

## Lifecycle

```ts
await runtime.destroy(counter); // drop one actor
await runtime.dispose();        // close actors (dispose/close if present), drain, terminate workers
await runtime.dispose({ closeActors: false });
```

After `dispose`, further `spawn` / method calls fail with a clear error.

## Passing actors as arguments

Proxies can be passed into methods (including across workers):

```ts
await linker.link(counter);
await linker.readOther();
```

## Helpers

```ts
import { getActorHandle } from "@js-ak/remote-objects";

const handle = getActorHandle(counter); // { workerId, objectId } | undefined
```

## Limits

- Arguments and results must be structured-clone compatible (plus actor refs)
- You cannot read instance fields through the proxy — only call methods
- Overlapping calls to the **same** actor are queued (mailbox); different actors may run in parallel
- Actors are not migrated between workers; identity is fixed at spawn

## License

MIT
