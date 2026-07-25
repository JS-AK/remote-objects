import { actor } from "../../index.js";

export class MailboxActor {
	private busy = false;
	private maxSeen = 0;

	async work(ms: number): Promise<number> {
		if (this.busy) {
			throw new Error("overlapping calls");
		}
		this.busy = true;
		this.maxSeen += 1;
		const seen = this.maxSeen;

		await new Promise((resolve) => setTimeout(resolve, ms));
		this.busy = false;

		return seen;
	}
}

actor(MailboxActor, import.meta);
