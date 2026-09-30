import { describe, expect, it } from "vitest";
import { SECONDS_IN } from "@/lib/time";
import { MAX_TTL_SECONDS, MIN_TTL_SECONDS, ttlToSeconds } from "@/pages/apps/channels/embed-link-utils";

describe("ttlToSeconds", () => {
    it("returns the value unchanged for seconds", () => {
        expect(ttlToSeconds(90, "second")).toBe(90);
    });

    it("converts each unit to seconds", () => {
        expect(ttlToSeconds(1, "minute")).toBe(60);
        expect(ttlToSeconds(1, "hour")).toBe(3_600);
        expect(ttlToSeconds(7, "day")).toBe(604_800);
    });

    it("scales linearly with the value", () => {
        expect(ttlToSeconds(3, "hour")).toBe(3 * SECONDS_IN.hour);
    });

    it("maps the server bounds to selectable durations", () => {
        // 60s min = 1 minute, 30-day max = the largest offered unit at the cap.
        expect(ttlToSeconds(1, "minute")).toBe(MIN_TTL_SECONDS);
        expect(ttlToSeconds(30, "day")).toBe(MAX_TTL_SECONDS);
    });
});
