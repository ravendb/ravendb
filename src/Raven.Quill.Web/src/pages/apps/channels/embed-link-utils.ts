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

// Swaps the operator host's leading label (dashboard.* / api.*) for the given subdomain, falling back
// to the current origin when there is no subdomain to swap (dev/localhost / bare IP).
function originForSubdomain(label: string) {
    const { protocol, hostname, port } = window.location;
    const isIpV4 = /^\d{1,3}(\.\d{1,3}){3}$/.test(hostname);
    const dot = hostname.indexOf(".");
    if (dot <= 0 || isIpV4) {
        return window.location.origin;
    }
    const host = `${label}.${hostname.slice(dot + 1)}`;
    return `${protocol}//${host}${port ? `:${port}` : ""}`;
}

// Server-enforced bounds for minting embed links (see the embed-links mint contract). Shared by the
// generate dialog (range validation + defaults) and the API docs (documented ranges + example body).
export const MIN_TTL_SECONDS = 60;
export const MAX_TTL_SECONDS = 2_592_000;
export const MIN_INVOCATIONS = 1;
export const MAX_INVOCATIONS = 1_000_000;
export const DEFAULT_TTL_SECONDS = 3_600;
export const DEFAULT_MAX_INVOCATIONS = 100;
