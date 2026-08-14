import type { CdcError } from "@/api/generated/server-api";

export type CdcErrorsSummary = {
    count: number;
    /** ISO timestamp of the newest stored error, or null when there is none to date. */
    latestAt: string | null;
};

/**
 * Condenses the app's stored CDC error log into the two facts both surfaces need: how many
 * errors were ever recorded, and when the newest one happened. The recency is what separates
 * this log from the live batch window on the data source page: that window holds only the
 * sink's last 25 batches, so a failure disappears from it while this log still lists it.
 */
export function summarizeCdcErrors(errors: CdcError[] | undefined): CdcErrorsSummary {
    if (!errors || errors.length === 0) {
        return { count: 0, latestAt: null };
    }

    let latestAt: string | null = null;
    let latestMs = -Infinity;

    for (const error of errors) {
        const createdMs = Date.parse(error.createdAt);
        if (!Number.isNaN(createdMs) && createdMs > latestMs) {
            latestMs = createdMs;
            latestAt = error.createdAt;
        }
    }

    return { count: errors.length, latestAt };
}
