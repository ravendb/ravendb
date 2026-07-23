import { useState, type ComponentProps } from "react";
import { Bar, BarChart, CartesianGrid, XAxis, YAxis } from "recharts";
import { ApiState } from "@/components/data/api-state";
import { ZERO_SAFE_Y_DOMAIN } from "@/lib/chart-domain";
import { Badge } from "@/components/shadcn/ui/badge";
import { Button } from "@/components/shadcn/ui/button";
import { ChartContainer, ChartTooltip, type ChartConfig } from "@/components/shadcn/ui/chart";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/shadcn/ui/select";
import { formatCompact } from "@/lib/format";
import { formatDateTime } from "@/lib/utils";
import { DashboardStatCards, type DashboardStatCard } from "@/pages/dashboard/dashboard-stat-cards";
import { CdcErrorsSheet } from "@/pages/apps/cdc-errors-sheet";
import { SectionCard } from "@/pages/apps/section-card";
import {
    MAX_TRACKED_BATCHES,
    useCdcLivePerformance,
    type CdcLiveBatch,
    type CdcLivePerformance,
    type CdcLiveStatus,
} from "@/pages/apps/use-cdc-live-performance";

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

    return (
        <SectionCard
            title={title}
            action={
                live.performance && (
                    <div className="flex items-center gap-2">
                        {live.performance.errorCount > 0 && (
                            <CdcErrorsSheet
                                slug={slug}
                                trigger={
                                    <Button variant="destructive-outline" size="sm">
                                        View errors
                                    </Button>
                                }
                            />
                        )}
                        <CdcStatusBadge status={live.performance.status} />
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

const CDC_STATUS_BADGES: Record<CdcLiveStatus, { variant: ComponentProps<typeof Badge>["variant"]; label: string }> = {
    active: { variant: "success", label: "Active" },
    idle: { variant: "secondary", label: "Idle" },
    error: { variant: "destructive", label: "Error" },
};

function CdcStatusBadge({ status }: { status: CdcLiveStatus }) {
    const badge = CDC_STATUS_BADGES[status];
    return <Badge variant={badge.variant}>{badge.label}</Badge>;
}

const CHART_LIMIT_OPTIONS = [50, 100, 250, MAX_TRACKED_BATCHES];
const DEFAULT_CHART_LIMIT = 100;

function CdcPerformanceContent({ performance }: { performance: CdcLivePerformance }) {
    const [chartLimit, setChartLimit] = useState(DEFAULT_CHART_LIMIT);
    const visibleBatches = performance.recentBatches.slice(-chartLimit);

    const cards: DashboardStatCard[] = [
        { label: "Recent writes", value: performance.recentWrites, isLoading: false },
        { label: "Errors", value: performance.errorCount, isLoading: false },
    ];

    return (
        <div className="space-y-4">
            <DashboardStatCards cards={cards} />
            <div className="flex items-center justify-between gap-2">
                <p className="text-sm text-muted-foreground">
                    Showing {visibleBatches.length} of {performance.totalBatches} batches
                </p>
                <Select value={String(chartLimit)} onValueChange={(value) => setChartLimit(Number(value))}>
                    <SelectTrigger size="sm" aria-label="Batches shown in chart" className="w-auto">
                        <SelectValue />
                    </SelectTrigger>
                    <SelectContent align="end">
                        {CHART_LIMIT_OPTIONS.map((option) => (
                            <SelectItem key={option} value={String(option)}>
                                Up to {option} batches
                            </SelectItem>
                        ))}
                    </SelectContent>
                </Select>
            </div>
            <CdcBatchesChart batches={visibleBatches} />
        </div>
    );
}

// recharts 3.x drops `Cell` fills inside `Bar`, so per-batch coloring is done with
// stacked series where each batch populates exactly one of them.
const batchesChartConfig = {
    okProcessed: { label: "Processed", color: "var(--success)" },
    inProgressProcessed: { label: "In progress", color: "var(--text-muted)" },
    errorProcessed: { label: "Processed (errors)", color: "var(--destructive)" },
} satisfies ChartConfig;

const batchTimeFormatter = new Intl.DateTimeFormat("en-US", { hour: "2-digit", minute: "2-digit", hour12: false });

function formatBatchTick(started: string) {
    const date = new Date(started);
    return Number.isNaN(date.getTime()) ? started : batchTimeFormatter.format(date);
}

function CdcBatchesChart({ batches }: { batches: CdcLiveBatch[] }) {
    if (batches.length === 0) {
        return <p className="py-8 text-center text-sm text-muted-foreground">No recent batches.</p>;
    }

    const points = batches.map((batch) => {
        const isInProgress = batch.ended === null;
        return {
            ...batch,
            okProcessed: batch.errors === 0 && !isInProgress ? batch.processed : null,
            inProgressProcessed: batch.errors === 0 && isInProgress ? batch.processed : null,
            errorProcessed: batch.errors > 0 ? batch.processed : null,
        };
    });

    return (
        <ChartContainer config={batchesChartConfig} className="aspect-auto h-56 w-full">
            <BarChart
                accessibilityLayer
                data={points}
                maxBarSize={24}
                margin={{ top: 8, right: 0, bottom: 0, left: 0 }}
            >
                <CartesianGrid vertical={false} />
                <XAxis
                    dataKey="started"
                    tickLine={false}
                    axisLine={false}
                    tickMargin={8}
                    tickFormatter={formatBatchTick}
                />
                <YAxis hide domain={ZERO_SAFE_Y_DOMAIN} />
                <ChartTooltip cursor={false} content={<CdcBatchTooltip />} />
                <Bar dataKey="okProcessed" stackId="batch" fill="var(--color-okProcessed)" radius={[4, 4, 0, 0]} />
                <Bar
                    dataKey="inProgressProcessed"
                    stackId="batch"
                    fill="var(--color-inProgressProcessed)"
                    radius={[4, 4, 0, 0]}
                />
                <Bar
                    dataKey="errorProcessed"
                    stackId="batch"
                    fill="var(--color-errorProcessed)"
                    radius={[4, 4, 0, 0]}
                />
            </BarChart>
        </ChartContainer>
    );
}

function CdcBatchTooltip({ active, payload }: { active?: boolean; payload?: ReadonlyArray<{ payload?: unknown }> }) {
    const batch = payload?.[0]?.payload as CdcLiveBatch | undefined;
    if (!active || !batch) {
        return null;
    }

    return (
        <div className="grid min-w-40 gap-1.5 rounded-lg border border-border/50 bg-background px-2.5 py-1.5 text-xs shadow-xl">
            <CdcBatchTooltipRow label="Started" value={formatDateTime(batch.started)} />
            <CdcBatchTooltipRow label="Ended" value={batch.ended ? formatDateTime(batch.ended) : "In progress"} />
            <CdcBatchTooltipRow label="Processed" value={formatCompact(batch.processed)} />
            <CdcBatchTooltipRow label="Duration" value={`${Math.round(batch.durationInMs)} ms`} />
            {batch.errors > 0 && <CdcBatchTooltipRow label="Errors" value={String(batch.errors)} />}
        </div>
    );
}

function CdcBatchTooltipRow({ label, value }: { label: string; value: string }) {
    return (
        <div className="flex items-center justify-between gap-4 leading-none">
            <span className="text-muted-foreground">{label}</span>
            <span className="font-mono font-medium text-foreground tabular-nums">{value}</span>
        </div>
    );
}
