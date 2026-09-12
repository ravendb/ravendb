import { MS_IN } from "@/lib/time";

// "normal" links need no attention; "warning" links are close to expiry/limit;
// "critical" links have already expired or exhausted their invocations.
export type EmbedLinkStatusTone = "normal" | "warning" | "critical";

// A link within this window of its expiry is flagged as expiring soon.
const EXPIRY_SOON_MS = MS_IN.day;
// Fraction of the invocation cap at which usage is flagged as nearing the limit.
const USAGE_WARNING_RATIO = 0.8;

export type EmbedLinkStatus = {
    tone: EmbedLinkStatusTone;
    // Short text shown in the cell.
    label: string;
    // Longer explanation surfaced as a tooltip.
    title: string;
};

export function getExpiryTone(expiresAt: string, now: number = Date.now()): EmbedLinkStatusTone {
    const expiry = new Date(expiresAt).getTime();
    if (Number.isNaN(expiry)) {
        return "normal";
    }

    const remainingMs = expiry - now;
    if (remainingMs <= 0) {
        return "critical";
    }
    return remainingMs <= EXPIRY_SOON_MS ? "warning" : "normal";
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
