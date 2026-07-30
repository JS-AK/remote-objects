import { fileURLToPath } from "node:url";
import path from "node:path";

/**
 * Directory of this compiled module (CJS emit patched in scripts/fix-cjs-import-meta.js).
 * @returns Absolute directory path of this file
 */
export function getModuleDir(): string {
	// eslint-disable-next-line @typescript-eslint/ban-ts-comment -- CJS rejects import.meta; postbuild rewrites emit
	// @ts-ignore
	return path.dirname(fileURLToPath(import.meta.url));
}
