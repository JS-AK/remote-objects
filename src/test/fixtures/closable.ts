import { actor } from "../../index.js";

/** Module-level probe shared by actors on the same worker. */
let lastClosed = false;

export class Closable {
	closed = false;

	close(): void {
		this.closed = true;
		lastClosed = true;
	}

	isClosed(): boolean {
		return this.closed;
	}
}

export class ClosableProbe {
	reset(): void {
		lastClosed = false;
	}

	wasLastClosed(): boolean {
		return lastClosed;
	}
}

actor(Closable, import.meta);
actor(ClosableProbe, import.meta);
