import { actor } from "../../index.js";

export class Closable {
	closed = false;

	close(): void {
		this.closed = true;
	}

	isClosed(): boolean {
		return this.closed;
	}
}

actor(Closable, import.meta);
