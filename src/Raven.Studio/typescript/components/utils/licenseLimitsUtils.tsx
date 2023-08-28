export type LicenseLimitReachStatus = "notReached" | "closeToLimit" | "limitReached";

function calculateThreshold(limit: number): number {
    return Math.max(Math.floor(0.8 * limit), limit - 8);
}

export function getLicenseLimitReachStatus(count: number, limit: number): LicenseLimitReachStatus {
    if (limit >= 0) {
        if (count >= limit) {
            return "limitReached";
        } else if (count >= calculateThreshold(limit)) {
            return "closeToLimit";
        } else {
            return "notReached";
        }
    } else {
        return "notReached"; // Unlimited
    }
}
