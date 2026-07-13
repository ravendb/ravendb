import { formatDateTime, formatRelativeTime } from "@/lib/utils";

// "normal" links need no attention; "warning" links are close to expiry/limit;
// "critical" links have already expired or exhausted their invocations.
export type EmbedLinkStatusTone = "normal" | "warning" | "critical";

// A link within this window of its expiry is flagged as expiring soon.
const EXPIRY_SOON_MS = 24 * 60 * 60 * 1000;
// Fraction of the invocation cap at which usage is flagged as nearing the limit.
const USAGE_WARNING_RATIO = 0.8;

export type EmbedLinkStatus = {
    tone: EmbedLinkStatusTone;
    // Short text shown in the cell.
    label: string;
    // Longer explanation surfaced as a tooltip.
    title: string;
};

export function getExpiryStatus(expiresAt: string, now: number = Date.now()): EmbedLinkStatus {
    const expiry = new Date(expiresAt).getTime();
    if (Number.isNaN(expiry)) {
        return { tone: "normal", label: expiresAt, title: expiresAt };
    }

    const remainingMs = expiry - now;
    if (remainingMs <= 0) {
        return { tone: "critical", label: "Expired", title: `Expired ${formatDateTime(expiresAt)}` };
    }

    const tone: EmbedLinkStatusTone = remainingMs <= EXPIRY_SOON_MS ? "warning" : "normal";
    return { tone, label: formatRelativeTime(expiresAt), title: `Expires ${formatDateTime(expiresAt)}` };
}

export function getUsageStatus(invocationCount: number, maxInvocations: number): EmbedLinkStatus {
    const label = `${invocationCount.toLocaleString()} / ${maxInvocations.toLocaleString()}`;
    const remaining = Math.max(0, maxInvocations - invocationCount);
    const isExhausted = maxInvocations <= 0 || invocationCount >= maxInvocations;

    const title = isExhausted
        ? `Limit reached · ${invocationCount.toLocaleString()} of ${maxInvocations.toLocaleString()} chats used`
        : `${invocationCount.toLocaleString()} of ${maxInvocations.toLocaleString()} chats used · ${remaining.toLocaleString()} left`;

    if (isExhausted) {
        return { tone: "critical", label, title };
    }

    const tone: EmbedLinkStatusTone = invocationCount / maxInvocations >= USAGE_WARNING_RATIO ? "warning" : "normal";
    return { tone, label, title };
}
