/** Builds the absolute, paste-ready embed URL from a token. The dashboard and the
 *  /embed surface are same-origin (proxied in dev, co-served in production), so the
 *  dashboard's own origin is the host the customer reaches the appliance at. */
export function buildEmbedUrl(token: string) {
    return `${window.location.origin}/embed/${token}`;
}
