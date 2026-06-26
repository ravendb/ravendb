import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import type { CdcPerformanceResponse } from "@/api/generated/server-api";
import { ApiState } from "@/components/data/api-state";
import { Badge } from "@/components/shadcn/ui/badge";
import { TableCell, TableRow } from "@/components/shadcn/ui/table";
import { formatCompact } from "@/lib/format";
import { formatDateTime } from "@/lib/utils";
import { DashboardStatCards, type DashboardStatCard } from "@/pages/dashboard/dashboard-stat-cards";
import { SectionCard, SectionTable } from "@/pages/apps/section-card";

export function CdcPerformanceSection({ slug }: { slug: string }) {
    const cdcQuery = useQuery(api.queries.apps.cdcPerformance(slug));

    return (
        <SectionCard title="CDC performance" action={cdcQuery.data && <CdcStatusBadge performance={cdcQuery.data} />}>
            <ApiState
                isLoading={cdcQuery.isPending}
                isError={cdcQuery.isError}
                errorTitle="Could not load CDC performance"
                onRetry={() => void cdcQuery.refetch()}
                loadingLabel="Loading CDC performance..."
            >
                {cdcQuery.data && <CdcPerformanceContent performance={cdcQuery.data} />}
            </ApiState>
        </SectionCard>
    );
}

function CdcStatusBadge({ performance }: { performance: CdcPerformanceResponse }) {
    if (!performance.enabled) {
        return <Badge variant="secondary">Disabled</Badge>;
    }
    if (performance.status.toLowerCase() === "running") {
        return <Badge variant="success">Running</Badge>;
    }
    return <Badge variant="warning">{performance.status}</Badge>;
}

function CdcPerformanceContent({ performance }: { performance: CdcPerformanceResponse }) {
    const cards: DashboardStatCard[] = [
        { label: "Replication lag", value: performance.lagSeconds ?? undefined, isLoading: false, caption: "seconds" },
        { label: "Recent reads", value: performance.recentReads, isLoading: false },
        { label: "Recent writes", value: performance.recentWrites, isLoading: false },
        { label: "Errors", value: performance.errorCount, isLoading: false },
    ];

    return (
        <div className="space-y-4">
            <DashboardStatCards cards={cards} />
            <SectionTable
                headers={["Started", "Duration", "Read", "Processed", "Errors", "Stop reason"]}
                isEmpty={performance.recentBatches.length === 0}
                emptyMessage="No recent batches."
            >
                {performance.recentBatches.map((batch) => (
                    <TableRow key={batch.started}>
                        <TableCell className="whitespace-nowrap text-muted-foreground">
                            {formatDateTime(batch.started)}
                        </TableCell>
                        <TableCell className="tabular-nums">{Math.round(batch.durationInMs)} ms</TableCell>
                        <TableCell className="tabular-nums">{formatCompact(batch.read)}</TableCell>
                        <TableCell className="tabular-nums">{formatCompact(batch.processed)}</TableCell>
                        <TableCell className="tabular-nums">{batch.errors}</TableCell>
                        <TableCell className="text-muted-foreground">{batch.stopReason ?? "—"}</TableCell>
                    </TableRow>
                ))}
            </SectionTable>
        </div>
    );
}
