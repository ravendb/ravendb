import type { CdcBatchPoint } from "@/api/generated/server-api";
import { toActivityDots } from "@/pages/apps/sync-activity";
import { describe, expect, it } from "vitest";

function batch(processed: number, errors = 0): CdcBatchPoint {
    return {
        started: "2026-08-14T09:00:00Z",
        completed: "2026-08-14T09:00:01Z",
        durationInMs: 1_000,
        read: processed + errors,
        processed,
        errors,
        stopReason: null,
    };
}

describe("toActivityDots", () => {
    it("draws nothing for an empty window", () => {
        expect(toActivityDots([])).toEqual([]);
    });

    it("scales each batch against the busiest one in the window", () => {
        const dots = toActivityDots([batch(0), batch(50), batch(100)]);

        expect(dots.map((dot) => dot.opacity)).toEqual([0.15, 0.57, 1]);
    });

    it("gives a lone batch the full ramp, whatever it processed", () => {
        expect(toActivityDots([batch(7)])[0]?.opacity).toBe(1);
    });

    // Without a guard this divides by zero, and every dot renders as NaN.
    it("keeps every dot visible when no batch in the window moved anything", () => {
        const dots = toActivityDots([batch(0), batch(0)]);

        expect(dots.map((dot) => dot.opacity)).toEqual([0.15, 0.15]);
    });

    it("marks a batch that reported errors", () => {
        const dots = toActivityDots([batch(100), batch(80, 3)]);

        expect(dots.map((dot) => dot.hasErrors)).toEqual([false, true]);
    });
});
