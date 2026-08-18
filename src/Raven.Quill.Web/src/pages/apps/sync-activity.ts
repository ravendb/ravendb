import type { CdcBatchPoint } from "@/api/generated/server-api";

export type ActivityDot = {
    /** 0-1, scaled against the busiest batch in the window. */
    opacity: number;
    hasErrors: boolean;
};

// A batch that moved nothing still happened, so every dot stays visible. The ramp above the floor
// is what carries volume, and it has to be wide: at this mark size a narrower range is not
// perceptible, and the dots collapse into a uniform dotted line.
const MIN_OPACITY = 0.15;
const MAX_OPACITY = 1;

/**
 * Turns the sink's recent batches into one dot each, scaled against the busiest batch in the
 * window rather than an absolute figure: the window is whatever the sink last did, so there is no
 * fixed ceiling to measure against, and a quiet app would otherwise render as a blank row.
 */
export function toActivityDots(batches: CdcBatchPoint[]): ActivityDot[] {
    const busiest = Math.max(0, ...batches.map((batch) => batch.processed));

    return batches.map((batch) => ({
        opacity: toOpacity(batch.processed, busiest),
        hasErrors: batch.errors > 0,
    }));
}

function toOpacity(processed: number, busiest: number): number {
    // Every batch in the window processed nothing, so there is no ramp to scale against.
    const ratio = busiest === 0 ? 0 : clamp(processed / busiest);
    return Number((MIN_OPACITY + (MAX_OPACITY - MIN_OPACITY) * ratio).toFixed(2));
}

function clamp(ratio: number): number {
    return Number.isNaN(ratio) ? 0 : Math.min(1, Math.max(0, ratio));
}
