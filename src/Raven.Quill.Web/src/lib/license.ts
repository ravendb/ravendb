import { startOfDay } from "date-fns";
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

// Day one of this setup: the server behind it has never run before this date, so no data
// can predate it. Returns undefined for the dates a server without a usable license
// reports (missing, unparsable, the 0001-01-01 default, or a clock-skewed future date),
// because a bogus lower bound would block more of the calendar than it should.
export function getSetupStartDate(license: ServerLicenseResponse | undefined): Date | undefined {
    if (!license?.firstServerStartDate) {
        return undefined;
    }
    const startDate = new Date(license.firstServerStartDate);
    if (Number.isNaN(startDate.getTime()) || startDate.getFullYear() < 2000 || startDate > new Date()) {
        return undefined;
    }
    return startOfDay(startDate);
}
