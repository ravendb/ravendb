// Swaps the operator host's leading label (dashboard.* / api.*) for the given subdomain, falling back
// to the current origin when there is no subdomain to swap (dev/localhost / bare IP).
export function originForSubdomain(label: string) {
    const { protocol, hostname, port } = window.location;
    const isIpV4 = /^\d{1,3}(\.\d{1,3}){3}$/.test(hostname);
    const dot = hostname.indexOf(".");
    if (dot <= 0 || isIpV4) {
        return window.location.origin;
    }
    const host = `${label}.${hostname.slice(dot + 1)}`;
    return `${protocol}//${host}${port ? `:${port}` : ""}`;
}
