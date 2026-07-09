import type { ServerLicenseResponse } from "@/api/generated/server-api";

const DAY_MS = 24 * 60 * 60 * 1000;

// Days until the license expires, replacing the daysLeft the server used to send.
export function getLicenseDaysLeft(license: ServerLicenseResponse): number {
    if (license.expired) {
        return 0;
    }
    const expiration = new Date(license.expiration).getTime();
    if (Number.isNaN(expiration)) {
        return 0;
    }
    return Math.max(0, Math.ceil((expiration - Date.now()) / DAY_MS));
}
