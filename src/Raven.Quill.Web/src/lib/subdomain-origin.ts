import { z } from "zod";

const DEFAULT_CONTAINER_NAME = "quill";
const APPLIANCE_HOST_LABEL_COUNT = 4;

export function isIpV4(value: string): boolean {
    return z.ipv4().safeParse(value).success;
}

// Appliance hosts are <role>.<slug>.myquill.ai, served by a *.<slug>.myquill.ai wildcard cert, so the
// role is the only label safe to swap and the slug is also the Docker container name. Anything shorter
// (dev/localhost, a bare IP, an apex host) has no role label to swap and no cert to match, so it falls
// back to the current origin and the compose container name.
function applianceLabels(hostname: string): string[] | null {
    if (isIpV4(hostname)) {
        return null;
    }
    const labels = hostname.split(".");
    return labels.length >= APPLIANCE_HOST_LABEL_COUNT ? labels : null;
}

export function originForSubdomain(label: string) {
    const { protocol, hostname, port } = window.location;
    const labels = applianceLabels(hostname);
    if (!labels) {
        return window.location.origin;
    }
    const host = [label, ...labels.slice(1)].join(".");
    return `${protocol}//${host}${port ? `:${port}` : ""}`;
}

export function containerNameForHost(hostname: string): string {
    return applianceLabels(hostname)?.[1] ?? DEFAULT_CONTAINER_NAME;
}
