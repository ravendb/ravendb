// Duplicated from the app's `src/lib/utils.ts` on purpose: this bundle ships to third-party
// sites and must not reach outside the widget package.
export function tryParseJson<T>(value: string): T | null {
    try {
        return JSON.parse(value) as T;
    } catch {
        return null;
    }
}
