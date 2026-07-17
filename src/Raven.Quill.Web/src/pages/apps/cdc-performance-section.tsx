/* eslint-disable react-hooks/incompatible-library */
"use no memo";

import type { ComponentProps } from "react";
import { getCoreRowModel, useReactTable, type ColumnDef } from "@tanstack/react-table";
import { ApiState } from "@/components/data/api-state";
import { Badge } from "@/components/shadcn/ui/badge";
import { VirtualDataTable } from "@/components/table/virtual-data-table";
import { formatCompact, formatDuration } from "@/lib/format";
import { formatDateTime } from "@/lib/utils";
import { DashboardStatCards, type DashboardStatCard } from "@/pages/dashboard/dashboard-stat-cards";
import { SectionCard } from "@/pages/apps/section-card";
import {
    useCdcLivePerformance,
    type CdcLiveBatch,
    type CdcLivePerformance,
    type CdcLiveStatus,
} from "@/pages/apps/use-cdc-live-performance";

export function CdcPerformanceSection({ slug }: { slug: string }) {
    const live = useCdcLivePerformance(slug);

    return (
        <SectionCard
            title="Live CDC performance"
            action={live.performance && <CdcStatusBadge status={live.performance.status} />}
        >
            <ApiState
                isLoading={live.connection === "connecting"}
                isError={live.connection === "error"}
                errorTitle="Could not connect to the live CDC feed"
                onRetry={live.retry}
                loadingLabel="Connecting to live CDC performance..."
            >
                {live.performance && <CdcPerformanceContent performance={live.performance} />}
            </ApiState>
        </SectionCard>
    );
}

const CDC_STATUS_BADGES: Record<CdcLiveStatus, { variant: ComponentProps<typeof Badge>["variant"]; label: string }> = {
    active: { variant: "success", label: "Active" },
    idle: { variant: "secondary", label: "Idle" },
    error: { variant: "destructive", label: "Error" },
};

function CdcStatusBadge({ status }: { status: CdcLiveStatus }) {
    const badge = CDC_STATUS_BADGES[status];
    return <Badge variant={badge.variant}>{badge.label}</Badge>;
}

const batchColumns: ColumnDef<CdcLiveBatch>[] = [
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

function CdcPerformanceContent({ performance }: { performance: CdcLivePerformance }) {
    const cards: DashboardStatCard[] = [
        {
            label: "Replication lag",
            value: performance.lagSeconds ?? undefined,
            valueLabel: performance.lagSeconds == null ? undefined : formatDuration(performance.lagSeconds),
            isLoading: false,
        },
        { label: "Recent writes", value: performance.recentWrites, isLoading: false },
        { label: "Errors", value: performance.errorCount, isLoading: false },
    ];

    const table = useReactTable({
        columns: batchColumns,
        data: performance.recentBatches,
        getCoreRowModel: getCoreRowModel(),
        getRowId: (batch) => batch.key,
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
