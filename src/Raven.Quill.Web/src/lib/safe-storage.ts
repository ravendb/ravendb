// Touching localStorage throws outright where site data is blocked - Safari's "Block all cookies",
// enterprise policies, a sandboxed iframe. Everything the dashboard keeps there is a remembered
// preference, never worth taking the app down for, so a blocked store reads empty and drops writes.

export function readStoredValue(key: string): string | null {
    try {
        return localStorage.getItem(key);
    } catch {
        return null;
    }
}

export function writeStoredValue(key: string, value: string) {
    try {
        localStorage.setItem(key, value);
    } catch {
        // Nothing to recover: the preference simply does not survive this session.
    }
}

export function isLocalStorageEvent(event: StorageEvent) {
    try {
        return event.storageArea === localStorage;
    } catch {
        return false;
    }
}
