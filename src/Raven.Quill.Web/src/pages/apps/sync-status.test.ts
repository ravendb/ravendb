import { hasErroredSinceLastSync, toSyncStatus } from "@/pages/apps/sync-status";
import { describe, expect, it } from "vitest";

const NOW_MS = Date.parse("2026-08-14T10:00:00Z");
const MINUTE = 60;
const HOUR = 60 * MINUTE;
const WEEK = 7 * 24 * HOUR;

describe("toSyncStatus", () => {
    it("keeps a status the badge knows", () => {
        expect(toSyncStatus("disabled")).toBe("disabled");
    });

    it("falls back to the neutral status for anything else", () => {
        expect(toSyncStatus("something-new")).toBe("idle");
    });
});

describe("hasErroredSinceLastSync", () => {
    it("reports no failure when nothing is on record", () => {
        expect(hasErroredSinceLastSync(null, 2 * MINUTE, NOW_MS)).toBe(false);
    });

    // The case the reported status cannot see: a sink that cannot connect writes errors without
    // ever producing a batch, so it keeps reporting "idle" while it fails hourly.
    it("reports a failure when the newest error is newer than the last sync", () => {
        expect(hasErroredSinceLastSync("2026-08-14T09:00:00Z", WEEK, NOW_MS)).toBe(true);
    });

    it("reports no failure when the sink synced after the newest error", () => {
        expect(hasErroredSinceLastSync("2026-07-21T08:14:02Z", 2 * MINUTE, NOW_MS)).toBe(false);
    });

    it("treats any error as a failure when nothing has ever synced", () => {
        expect(hasErroredSinceLastSync("2026-07-21T08:14:02Z", null, NOW_MS)).toBe(true);
    });

    it("reports no failure when nothing has ever synced and nothing ever failed", () => {
        expect(hasErroredSinceLastSync(null, null, NOW_MS)).toBe(false);
    });

    it("ignores an error timestamp it cannot parse", () => {
        expect(hasErroredSinceLastSync("not-a-date", WEEK, NOW_MS)).toBe(false);
    });
});
