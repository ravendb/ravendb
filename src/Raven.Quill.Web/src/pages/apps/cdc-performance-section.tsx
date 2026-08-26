import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
import { StatusIndicator, type StatusTone } from "@/components/data/status-indicator";
import { CdcBatchLog } from "@/pages/apps/cdc-batch-log";
import { DashboardStatCards, type DashboardStatCard } from "@/pages/dashboard/dashboard-stat-cards";
import { CdcErrorsAlert } from "@/pages/apps/cdc-errors-alert";
import { SectionCard } from "@/pages/apps/section-card";
import {
    useCdcLivePerformance,
    type CdcLivePerformance,
    type CdcLiveStatus,
} from "@/pages/apps/use-cdc-live-performance";

export function CdcPerformanceSection({
    slug,
    title = "Sync performance",
    loadingLabel = "Connecting to the live data sync...",
    errorTitle = "Could not connect to the live data sync",
}: {
    slug: string;
    title?: string;
    loadingLabel?: string;
    errorTitle?: string;
}) {
    const live = useCdcLivePerformance(slug);
    const errorsQuery = useQuery(api.queries.apps.cdcErrors(slug));
    const storedErrorCount = errorsQuery.data?.length ?? 0;
    const hasStoredErrors = storedErrorCount > 0;

    // The live feed only counts errors inside the batches.
    // Failure that never produced a batch (e.g. source table that doesn't exist) reaches the stored list alone.
    // Reporting the larger of the two keeps the tile from claiming zero.
    const errorCount = Math.max(live.performance?.errorCount ?? 0, storedErrorCount);

    return (
        <SectionCard
            title={title}
            action={live.performance && <CdcStatusBadge status={hasStoredErrors ? "error" : live.performance.status} />}
        >
            <div className="space-y-4">
                <CdcErrorsAlert slug={slug} />
                <ApiState
                    isLoading={live.connection === "connecting"}
                    isError={live.connection === "error"}
                    errorTitle={errorTitle}
                    onRetry={live.retry}
                    loadingLabel={loadingLabel}
                >
                    {live.performance && (
                        <CdcPerformanceContent performance={live.performance} errorCount={errorCount} />
                    )}
                </ApiState>
            </div>
        </SectionCard>
    );
}

const CDC_STATUS_BADGES: Record<CdcLiveStatus, { tone: StatusTone; label: string }> = {
    active: { tone: "positive", label: "Active" },
    idle: { tone: "muted", label: "Idle" },
    error: { tone: "danger", label: "Error" },
};

function CdcStatusBadge({ status }: { status: CdcLiveStatus }) {
    const badge = CDC_STATUS_BADGES[status];
    return <StatusIndicator tone={badge.tone} label={badge.label} />;
}

function CdcPerformanceContent({ performance, errorCount }: { performance: CdcLivePerformance; errorCount: number }) {
    const cards: DashboardStatCard[] = [
        { label: "Recent writes", value: performance.recentWrites, isLoading: false },
        { label: "Errors", value: errorCount, isLoading: false },
    ];

    return (
        <div className="space-y-4">
            <DashboardStatCards cards={cards} />
            <CdcBatchLog batches={performance.batches} />
        </div>
    );
}
