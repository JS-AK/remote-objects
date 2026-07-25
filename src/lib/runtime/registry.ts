import type { AnyActorClass } from "../types.js";

type ClassRegistration = {
	className: string;
	moduleUrl: string;
	exportName: string;
};

type Entry = ClassRegistration & {
	Class: AnyActorClass;
};

export class Registry {
	private readonly classes = new Map<string, Entry>();

	register(
		Class: AnyActorClass,
		moduleUrl: string,
		exportName: string = Class.name,
	): void {
		this.classes.set(Class.name, {
			Class,
			className: Class.name,
			exportName,
			moduleUrl,
		});
	}

	get(name: string): Entry | undefined {
		return this.classes.get(name);
	}

	has(name: string): boolean {
		return this.classes.has(name);
	}
}
