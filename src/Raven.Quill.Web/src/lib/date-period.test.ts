import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
    clampPeriod,
    drillInto,
    isSameDatePeriod,
    parseStartDate,
    stepDay,
    stepMonth,
    stepYear,
} from "@/lib/date-period";
import { getSetupStartDate } from "@/lib/license";
import type { ServerLicenseResponse } from "@/api/generated/server-api";

const NOW = new Date(2026, 7, 12, 10, 30);
const SETUP_START = new Date(2026, 5, 14);

beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
});

afterEach(() => {
    vi.useRealTimers();
});

describe("clampPeriod", () => {
    it("keeps a period that is inside the range", () => {
        expect(clampPeriod({ year: 2026, month: 7, day: 3 }, SETUP_START)).toEqual({ year: 2026, month: 7, day: 3 });
    });

    it("pulls a year before the setup up to the setup year", () => {
        expect(clampPeriod({ year: 2024, month: null, day: null }, SETUP_START)).toEqual({
            year: 2026,
            month: null,
            day: null,
        });
    });

    it("pulls a month before the setup up to the setup month", () => {
        expect(clampPeriod({ year: 2026, month: 2, day: null }, SETUP_START)).toEqual({
            year: 2026,
            month: 6,
            day: null,
        });
    });

    it("pulls a day before the setup up to the setup day", () => {
        expect(clampPeriod({ year: 2026, month: 6, day: 1 }, SETUP_START)).toEqual({ year: 2026, month: 6, day: 14 });
    });

    it("still caps at today", () => {
        expect(clampPeriod({ year: 2027, month: 12, day: 31 }, SETUP_START)).toEqual({
            year: 2026,
            month: 8,
            day: 12,
        });
    });

    it("caps at today when there is no lower bound", () => {
        expect(clampPeriod({ year: 2030, month: 1, day: null })).toEqual({ year: 2026, month: 8, day: null });
    });
});

describe("stepping at the lower bound", () => {
    it("holds the day at the setup day", () => {
        const atBound = { year: 2026, month: 6, day: 14 };
        expect(stepDay(atBound, -1, SETUP_START)).toEqual(atBound);
        expect(isSameDatePeriod(stepDay(atBound, -1, SETUP_START), atBound)).toBe(true);
    });

    it("holds the month at the setup month", () => {
        const atBound = { year: 2026, month: 6, day: null };
        expect(stepMonth(atBound, -1, SETUP_START)).toEqual(atBound);
    });

    it("holds the year at the setup year", () => {
        const atBound = { year: 2026, month: null, day: null };
        expect(stepYear(atBound, -1, SETUP_START)).toEqual(atBound);
    });

    it("still steps down while above the bound", () => {
        expect(stepMonth({ year: 2026, month: 8, day: null }, -1, SETUP_START)).toEqual({
            year: 2026,
            month: 7,
            day: null,
        });
    });
});

describe("drillInto", () => {
    it("clamps a pre-setup bucket to the setup day", () => {
        expect(drillInto({ year: 2026, month: 6, day: null }, "2026-06-02T00:00:00", SETUP_START)).toEqual({
            year: 2026,
            month: 6,
            day: 14,
        });
    });

    it("drills a year into the clicked month", () => {
        expect(drillInto({ year: 2026, month: null, day: null }, "2026-07-01T00:00:00", SETUP_START)).toEqual({
            year: 2026,
            month: 7,
            day: null,
        });
    });

    it("ignores a bucket that is not a date", () => {
        expect(drillInto({ year: 2026, month: 6, day: null }, "not-a-date")).toBeNull();
    });
});

// The lower bound on per-app views: the app's createdAt, as the server reports it.
describe("parseStartDate", () => {
    it("returns the start of the reported day", () => {
        expect(parseStartDate("2026-07-21T08:12:45Z")).toEqual(new Date(2026, 6, 21));
    });

    it("returns undefined when the server reports no date", () => {
        expect(parseStartDate(undefined)).toBeUndefined();
        expect(parseStartDate(null)).toBeUndefined();
        expect(parseStartDate("")).toBeUndefined();
    });

    it("returns undefined for a date no server could have reported", () => {
        expect(parseStartDate("0001-01-01T00:00:00")).toBeUndefined();
        expect(parseStartDate("not-a-date")).toBeUndefined();
        expect(parseStartDate("2027-01-01T00:00:00")).toBeUndefined();
    });
});

describe("getSetupStartDate", () => {
    const license = (firstServerStartDate: string): ServerLicenseResponse => ({
        errorMessage: "",
        expiration: "2026-12-01T00:00:00Z",
        subscriptionExpiration: "2026-12-01T00:00:00Z",
        expired: false,
        firstServerStartDate,
        id: "license-id",
        licensedTo: "Acme Corp",
        status: "Commercial",
        type: "Quill",
        version: "7.2",
    });

    it("returns the start of the reported day", () => {
        expect(getSetupStartDate(license("2026-06-14T09:12:41"))).toEqual(new Date(2026, 5, 14));
    });

    it("returns undefined without a license", () => {
        expect(getSetupStartDate(undefined)).toBeUndefined();
    });

    it("returns undefined for the dates a server without a license reports", () => {
        expect(getSetupStartDate(license(""))).toBeUndefined();
        expect(getSetupStartDate(license("0001-01-01T00:00:00"))).toBeUndefined();
        expect(getSetupStartDate(license("not-a-date"))).toBeUndefined();
    });

    it("returns undefined for a future date", () => {
        expect(getSetupStartDate(license("2027-01-01T00:00:00"))).toBeUndefined();
    });
});
