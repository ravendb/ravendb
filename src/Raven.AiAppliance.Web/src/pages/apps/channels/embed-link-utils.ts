/** Builds the absolute, paste-ready embed URL from a token. The embed surface is served on the
 *  public.* subdomain only, so swap the operator host's leading label (dashboard.* / api.*) for
 *  public.* — falling back to the current origin when there is no subdomain to swap (dev/localhost). */
export function buildEmbedUrl(token: string) {
    return `${publicOrigin()}/embed/${token}`;
}

function publicOrigin() {
    const { protocol, hostname, port } = window.location;
    const isIpV4 = /^\d{1,3}(\.\d{1,3}){3}$/.test(hostname);
    const dot = hostname.indexOf(".");
    if (dot <= 0 || isIpV4) {
        return window.location.origin;
    }
    const publicHost = `public.${hostname.slice(dot + 1)}`;
    return `${protocol}//${publicHost}${port ? `:${port}` : ""}`;
}
