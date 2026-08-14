import type { CdcBatchPoint } from "@/api/generated/server-api";
import { cn } from "@/lib/utils";
import { toActivityDots } from "@/pages/apps/sync-activity";

/**
 * An ambient reading of the sink's recent batches: one dot each, opacity carrying volume and a
 * failed batch marked. It is deliberately not precise enough to measure - the readings beside it
 * carry the facts - but every dot is a real batch, so it degrades into information rather than
 * texture.
 */
export function SyncActivityDots({ batches }: { batches: CdcBatchPoint[] }) {
    const dots = toActivityDots(batches);

    // A sink that cannot reach its source produces no batches at all, so this is the state it
    // lands in whenever something is badly wrong. Say so rather than rendering nothing.
    if (dots.length === 0) {
        return <p className="text-xs text-muted-foreground">No batches in the recent window</p>;
    }

    const erroredCount = dots.filter((dot) => dot.hasErrors).length;

    return (
        <div
            role="img"
            aria-label={`${dots.length} recent ${dots.length === 1 ? "batch" : "batches"}, ${erroredCount} with errors`}
            className="flex items-center gap-1"
        >
            {dots.map((dot, index) => (
                <span
                    key={index}
                    className={cn("h-2 w-1.5 shrink-0 rounded-full", dot.hasErrors ? "bg-destructive" : "bg-primary")}
                    // A failed batch keeps full strength: its colour is the point, not its volume.
                    style={dot.hasErrors ? undefined : { opacity: dot.opacity }}
                />
            ))}
        </div>
    );
}
