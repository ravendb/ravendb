/* eslint-disable react-hooks/incompatible-library */
"use no memo";

import type { ComponentProps } from "react";
import { useQuery } from "@tanstack/react-query";
import { getCoreRowModel, useReactTable, type ColumnDef } from "@tanstack/react-table";
import { api } from "@/api/api";
import type { CdcPerformanceResponse } from "@/api/generated/server-api";
import { ApiState } from "@/components/data/api-state";
import { Badge } from "@/components/shadcn/ui/badge";
import { VirtualDataTable } from "@/components/table/virtual-data-table";
import { formatCompact } from "@/lib/format";
import { formatDateTime } from "@/lib/utils";
import { DashboardStatCards, type DashboardStatCard } from "@/pages/dashboard/dashboard-stat-cards";
import { SectionCard } from "@/pages/apps/section-card";

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

// Maps the backend CDC status contract ("not-configured" | "disabled" | "idle" | "active"
// | "error") to a badge — a healthy "active" sink reads as success, not a warning.
const CDC_STATUS_BADGES: Record<string, { variant: ComponentProps<typeof Badge>["variant"]; label: string }> = {
    active: { variant: "success", label: "Active" },
    idle: { variant: "secondary", label: "Idle" },
    disabled: { variant: "secondary", label: "Disabled" },
    "not-configured": { variant: "outline", label: "Not configured" },
    error: { variant: "destructive", label: "Error" },
};

function CdcStatusBadge({ performance }: { performance: CdcPerformanceResponse }) {
    const badge = CDC_STATUS_BADGES[performance.status] ?? { variant: "warning" as const, label: performance.status };
    return <Badge variant={badge.variant}>{badge.label}</Badge>;
}

type CdcBatch = CdcPerformanceResponse["recentBatches"][number];

const batchColumns: ColumnDef<CdcBatch>[] = [
    {
        accessorKey: "started",
        header: "Started",
        cell: ({ getValue }) => <span className="text-muted-foreground">{formatDateTime(getValue<string>())}</span>,
    },
    {
        accessorKey: "durationInMs",
        header: "Duration",
        cell: ({ getValue }) => <span className="tabular-nums">{Math.round(getValue<number>())} ms</span>,
    },
    {
        accessorKey: "processed",
        header: "Processed",
        cell: ({ getValue }) => <span className="tabular-nums">{formatCompact(getValue<number>())}</span>,
    },
    {
        accessorKey: "errors",
        header: "Errors",
        cell: ({ getValue }) => <span className="tabular-nums">{getValue<number>()}</span>,
    },
];

function CdcPerformanceContent({ performance }: { performance: CdcPerformanceResponse }) {
    const cards: DashboardStatCard[] = [
        { label: "Replication lag", value: performance.lagSeconds ?? undefined, isLoading: false, caption: "seconds" },
        { label: "Recent writes", value: performance.recentWrites, isLoading: false },
        { label: "Errors", value: performance.errorCount, isLoading: false },
    ];

    const table = useReactTable({
        columns: batchColumns,
        data: performance.recentBatches,
        getCoreRowModel: getCoreRowModel(),
        getRowId: (batch, index) => `${batch.started}-${index}`,
    });

    return (
        <div className="space-y-4">
            <DashboardStatCards cards={cards} />
            <VirtualDataTable
                table={table}
                columnCount={batchColumns.length}
                emptyMessage="No recent batches."
                className="bg-card"
            />
        </div>
    );
}
