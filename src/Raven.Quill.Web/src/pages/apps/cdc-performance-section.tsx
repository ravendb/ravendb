import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
import { Button } from "@/components/shadcn/ui/button";
import { CdcBatchLog } from "@/pages/apps/cdc-batch-log";
import { DashboardStatCards, type DashboardStatCard } from "@/pages/dashboard/dashboard-stat-cards";
import { CdcErrorsSheet } from "@/pages/apps/cdc-errors-sheet";
import { summarizeCdcErrors } from "@/pages/apps/cdc-errors-summary";
import { SectionCard } from "@/pages/apps/section-card";
import { SyncStatusBadge } from "@/pages/apps/sync-status-badge";
import { useCdcLivePerformance, type CdcLivePerformance } from "@/pages/apps/use-cdc-live-performance";

export function CdcPerformanceSection({
    slug,
    title = "Live CDC performance",
    loadingLabel = "Connecting to live CDC performance...",
    errorTitle = "Could not connect to the live CDC feed",
}: {
    slug: string;
    title?: string;
    loadingLabel?: string;
    errorTitle?: string;
}) {
    const live = useCdcLivePerformance(slug);
    // The button opens the stored error list, so it is gated on that list rather than on the
    // live errorCount, which only covers the recently tracked batches and could disagree.
    // Both counts are shown side by side, each naming its own window, so the disagreement
    // reads as two different measurements rather than as one of them being wrong.
    const errorsQuery = useQuery(api.queries.apps.cdcErrors(slug));
    const storedErrorCount = summarizeCdcErrors(errorsQuery.data).count;

    return (
        <SectionCard
            title={
                <span className="flex items-center gap-2">
                    {title}
                    {live.performance && <SyncStatusBadge status={live.performance.status} />}
                </span>
            }
            description="Live view of the sink's most recent batches, extended as new ones complete."
            action={
                storedErrorCount > 0 && (
                    <CdcErrorsSheet
                        slug={slug}
                        trigger={
                            <Button variant="destructive-outline" size="sm">
                                View {storedErrorCount} recorded {storedErrorCount === 1 ? "error" : "errors"}
                            </Button>
                        }
                    />
                )
            }
        >
            <ApiState
                isLoading={live.connection === "connecting"}
                isError={live.connection === "error"}
                errorTitle={errorTitle}
                onRetry={live.retry}
                loadingLabel={loadingLabel}
            >
                {live.performance && <CdcPerformanceContent performance={live.performance} />}
            </ApiState>
        </SectionCard>
    );
}

function CdcPerformanceContent({ performance }: { performance: CdcLivePerformance }) {
    const cards: DashboardStatCard[] = [
        { label: "Recent CDC writes", value: performance.recentWrites, isLoading: false },
        {
            label: "Recent errors",
            labelInfo: "Errors in the batches listed below.",
            value: performance.errorCount,
            isLoading: false,
        },
    ];

    return (
        <div className="space-y-4">
            <DashboardStatCards cards={cards} />
            <CdcBatchLog batches={performance.batches} />
        </div>
    );
}
