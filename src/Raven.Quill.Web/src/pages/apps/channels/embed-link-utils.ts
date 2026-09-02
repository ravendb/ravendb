import { originForSubdomain } from "@/lib/subdomain-origin";

/** Builds the absolute, paste-ready embed URL from the app slug and a token. The embed surface is
 *  served on the public.* subdomain only, so swap the operator host's leading label (dashboard.* /
 *  api.*) for public.* — falling back to the current origin when there is no subdomain to swap
 *  (dev/localhost). Prefer the mint response's `url` when available; this rebuilds the same shape
 *  for links loaded from the list endpoint. */
export function buildEmbedUrl(slug: string, token: string) {
    return `${originForSubdomain("public")}/apps/${encodeURIComponent(slug)}/embed/${token}`;
}

/** Builds the absolute embed-links mint endpoint shown in the "Generate links via the API" docs. The
 *  API is served on the api.* subdomain, so swap the operator host's leading label for api.* — falling
 *  back to the current origin when there is no subdomain to swap (dev/localhost), where /api is local. */
export function buildMintEmbedLinkUrl(slug: string) {
    return `${originForSubdomain("api")}/api/apps/${encodeURIComponent(slug)}/embed-links`;
}

// Server-enforced bounds for minting embed links (see the embed-links mint contract). Shared by the
// generate dialog (range validation + defaults) and the API docs (documented ranges + example body).
export const MIN_TTL_SECONDS = 60;
export const MAX_TTL_SECONDS = 2_592_000;
export const MIN_INVOCATIONS = 1;
export const MAX_INVOCATIONS = 1_000_000;
export const DEFAULT_TTL_SECONDS = 3_600;
export const DEFAULT_MAX_INVOCATIONS = 100;

// Units offered by the custom-duration input. Days is the largest practical unit: the server caps
// TTL at MAX_TTL_SECONDS (30 days), so weeks/months could not be honored.
export const TTL_UNIT_SECONDS = {
    second: 1,
    minute: 60,
    hour: 3_600,
    day: 86_400,
} as const;

export type TtlUnit = keyof typeof TTL_UNIT_SECONDS;

export function ttlToSeconds(value: number, unit: TtlUnit) {
    return value * TTL_UNIT_SECONDS[unit];
}
