import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
import { StatusIndicator, type StatusTone } from "@/components/data/status-indicator";
import { Button } from "@/components/shadcn/ui/button";
import { CdcBatchLog } from "@/pages/apps/cdc-batch-log";
import { DashboardStatCards, type DashboardStatCard } from "@/pages/dashboard/dashboard-stat-cards";
import { CdcErrorsSheet } from "@/pages/apps/cdc-errors-sheet";
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
    // The button opens the stored error list, so it is gated on that list rather than on the
    // live errorCount, which only covers the recently tracked batches and could disagree.
    const errorsQuery = useQuery(api.queries.apps.cdcErrors(slug));
    const hasStoredErrors = (errorsQuery.data?.length ?? 0) > 0;

    return (
        <SectionCard
            title={title}
            action={
                (hasStoredErrors || live.performance) && (
                    <div className="flex items-center gap-2">
                        {hasStoredErrors && (
                            <CdcErrorsSheet
                                slug={slug}
                                trigger={
                                    <Button variant="destructive-outline" size="sm">
                                        View errors
                                    </Button>
                                }
                            />
                        )}
                        {live.performance && <CdcStatusBadge status={live.performance.status} />}
                    </div>
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

const CDC_STATUS_BADGES: Record<CdcLiveStatus, { tone: StatusTone; label: string }> = {
    active: { tone: "positive", label: "Active" },
    idle: { tone: "muted", label: "Idle" },
    error: { tone: "danger", label: "Error" },
};

function CdcStatusBadge({ status }: { status: CdcLiveStatus }) {
    const badge = CDC_STATUS_BADGES[status];
    return <StatusIndicator tone={badge.tone} label={badge.label} />;
}

function CdcPerformanceContent({ performance }: { performance: CdcLivePerformance }) {
    const cards: DashboardStatCard[] = [
        { label: "Recent writes", value: performance.recentWrites, isLoading: false },
        { label: "Errors", value: performance.errorCount, isLoading: false },
    ];

    return (
        <div className="space-y-4">
            <DashboardStatCards cards={cards} />
            <CdcBatchLog batches={performance.batches} />
        </div>
    );
}
