import type { CdcError } from "@/api/generated/server-api";
import { summarizeCdcErrors } from "@/pages/apps/cdc-errors-summary";
import { describe, expect, it } from "vitest";

function error(createdAt: string): CdcError {
    return {
        taskName: "cdc/demo-shop",
        createdAt,
        step: "Script processing",
        error: "boom",
        documentId: null,
        affectedDocumentsCount: null,
    };
}

describe("summarizeCdcErrors", () => {
    it("reports nothing while the errors are still loading", () => {
        expect(summarizeCdcErrors(undefined)).toEqual({ count: 0, latestAt: null });
    });

    it("reports nothing for an empty log", () => {
        expect(summarizeCdcErrors([])).toEqual({ count: 0, latestAt: null });
    });

    it("counts the stored errors and keeps the newest timestamp", () => {
        const summary = summarizeCdcErrors([error("2026-07-21T08:12:45Z"), error("2026-07-21T08:14:02Z")]);

        expect(summary).toEqual({ count: 2, latestAt: "2026-07-21T08:14:02Z" });
    });

    // The endpoint sorts newest-first, but nothing in the client enforces that, so the newest
    // timestamp has to be picked rather than read off the first entry.
    it("finds the newest timestamp regardless of the order it arrives in", () => {
        const ascending = [error("2026-07-21T08:12:45Z"), error("2026-07-21T08:12:47Z"), error("2026-07-21T08:14:02Z")];

        expect(summarizeCdcErrors(ascending).latestAt).toBe("2026-07-21T08:14:02Z");
    });

    it("still counts errors whose timestamp cannot be parsed", () => {
        expect(summarizeCdcErrors([error("not-a-date")])).toEqual({ count: 1, latestAt: null });
    });

    it("ignores unparseable timestamps when picking the newest one", () => {
        const summary = summarizeCdcErrors([error("not-a-date"), error("2026-07-21T08:14:02Z")]);

        expect(summary).toEqual({ count: 2, latestAt: "2026-07-21T08:14:02Z" });
    });
});
