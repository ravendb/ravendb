import { z } from "zod";

export function isIpV4(value: string): boolean {
    return z.ipv4().safeParse(value).success;
}

// Swaps the operator host's leading label (dashboard.* / api.*) for the given subdomain, falling back
// to the current origin when there is no subdomain to swap (dev/localhost / bare IP).
export function originForSubdomain(label: string) {
    const { protocol, hostname, port } = window.location;
    const dot = hostname.indexOf(".");
    if (dot <= 0 || isIpV4(hostname)) {
        return window.location.origin;
    }
    const host = `${label}.${hostname.slice(dot + 1)}`;
    return `${protocol}//${host}${port ? `:${port}` : ""}`;
}

const DEFAULT_CONTAINER_NAME = "quill";

// Appliance hosts look like <role>.<slug>.<domain>.<tld>, where the slug is also the Docker
// container name. Dev/localhost and bare-IP hosts carry no slug, so they fall back to the
// name the compose file uses.
export function containerNameForHost(hostname: string): string {
    if (isIpV4(hostname)) {
        return DEFAULT_CONTAINER_NAME;
    }
    const labels = hostname.split(".");
    return labels.length >= 4 ? labels[1] : DEFAULT_CONTAINER_NAME;
}
