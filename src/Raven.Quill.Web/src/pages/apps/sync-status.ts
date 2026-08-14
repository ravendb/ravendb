// What the sink reports. The live feed can only produce the first three, since it carries no
// disabled flag; the REST snapshot adds "disabled".
export const SYNC_STATUSES = ["active", "idle", "error", "disabled"] as const;

export type SyncStatus = (typeof SYNC_STATUSES)[number];

// The contract types status as a plain string, so an unrecognised value falls back to the
// neutral status rather than rendering undefined.
export function toSyncStatus(status: string): SyncStatus {
    return SYNC_STATUSES.includes(status as SyncStatus) ? (status as SyncStatus) : "idle";
}

/**
 * Whether the sink has failed since it last made progress.
 *
 * The reported status cannot answer this on its own. A connection-level failure is written
 * straight to the error store without producing a batch, so it leaves the rolling performance
 * window empty, which leaves the reported error count at zero and the status at "idle" however
 * long the sink has been failing. Comparing the two instants catches it: an error newer than the
 * last successful sync means the sink has not recovered from it.
 *
 * Both ages are measured against the same clock, so client skew cancels out of the comparison.
 */
export function hasErroredSinceLastSync(
    latestErrorAt: string | null,
    lagSeconds: number | null,
    nowMs: number,
): boolean {
    if (latestErrorAt === null) {
        return false;
    }

    const erroredAtMs = Date.parse(latestErrorAt);
    if (Number.isNaN(erroredAtMs)) {
        return false;
    }

    // Nothing ever synced, so any recorded error is the most recent thing that happened.
    if (lagSeconds === null) {
        return true;
    }

    return nowMs - erroredAtMs < lagSeconds * 1000;
}
