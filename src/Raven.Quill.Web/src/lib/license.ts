import type { ServerLicenseResponse } from "@/api/generated/server-api";
import { parseStartDate } from "@/lib/date-period";
import { MS_IN } from "@/lib/time";

// Days until the license expires, replacing the daysLeft the server used to send.
export function getLicenseDaysLeft(license: ServerLicenseResponse): number {
    if (license.expired) {
        return 0;
    }
    const expiration = new Date(license.expiration).getTime();
    if (Number.isNaN(expiration)) {
        return 0;
    }
    return Math.max(0, Math.ceil((expiration - Date.now()) / MS_IN.day));
}

// Day one of this setup: the server behind it has never run before this date, so nothing
// server-wide can predate it. Per-app views use the app's own creation date instead, which
// is always later - see useAppStartDate.
export function getSetupStartDate(license: ServerLicenseResponse | undefined): Date | undefined {
    return parseStartDate(license?.firstServerStartDate);
}
