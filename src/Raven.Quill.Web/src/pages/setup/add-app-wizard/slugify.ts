export const MAX_SLUG_LENGTH = 128;

// Mirrors the server's Slugifier.ToSlug (Raven.Quill/Wizard/Slugifier.cs)
export function toSlug(value: string) {
    return value
        .replace(/[^A-Za-z0-9]+/g, "-")
        .replace(/^-+|-+$/g, "")
        .toLowerCase();
}

/**
 * A throwaway slug for work that must reach the server before the app is named - the wizard endpoints
 * key their state by slug. Never shown to the operator and never kept, so it only has to be unlikely
 * to collide with another draft; crypto.randomUUID is avoided because it needs a secure context.
 */
export function createDraftSlug() {
    return `draft-${Math.random().toString(36).slice(2, 10)}`;
}
