import type { AnyActorClass } from "../types.js";

/** Module binding recorded for a registered actor class. */
type ClassRegistration = {
	className: string;
	moduleUrl: string;
	exportName: string;
};

/** Host-side registry entry: class constructor + load metadata. */
type Entry = ClassRegistration & {
	Class: AnyActorClass;
};

/**
 * Host-side map of actor class name → module URL / export.
 * Used to know which classes are already registered before spawn.
 */
export class Registry {
	private readonly classes = new Map<string, Entry>();

	/**
	 * Records a class binding (does not talk to workers by itself).
	 * @param Class - Actor class
	 * @param moduleUrl - Absolute URL workers use to `import()` the module
	 * @param exportName - Named export inside that module (default: `Class.name`)
	 */
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

	/**
	 * @param name - Class name key
	 * @returns Entry if registered, otherwise `undefined`
	 */
	get(name: string): Entry | undefined {
		return this.classes.get(name);
	}

	/**
	 * @param name - Class name key
	 * @returns Whether the class is already recorded
	 */
	has(name: string): boolean {
		return this.classes.has(name);
	}
}
