export type LicenseLimitReachStatus = "notReached" | "closeToLimit" | "limitReached";

function calculateThreshold(limit: number): number {
    return Math.max(Math.floor(0.8 * limit), limit - 8);
}

export function getLicenseLimitReachStatus(count: number, limit: number): LicenseLimitReachStatus {
    if (!count || !limit) {
        return "notReached";
    }

    if (count >= limit) {
        return "limitReached";
    }

    if (count >= calculateThreshold(limit)) {
        return "closeToLimit";
    }

    return "notReached";
}
