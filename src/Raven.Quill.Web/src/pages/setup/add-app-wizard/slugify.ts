export const MAX_SLUG_LENGTH = 128;

// Mirrors the server's Slugifier.ToSlug (Raven.Quill/Wizard/Slugifier.cs)
export function toSlug(value: string) {
    return value
        .replace(/[^A-Za-z0-9]+/g, "-")
        .replace(/^-+|-+$/g, "")
        .toLowerCase();
}
