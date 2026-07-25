import { actor } from "../../index.js";

export class SlowActor {
	async wait(ms: number): Promise<string> {
		await new Promise((resolve) => setTimeout(resolve, ms));

		return "done";
	}
}

actor(SlowActor, import.meta);
