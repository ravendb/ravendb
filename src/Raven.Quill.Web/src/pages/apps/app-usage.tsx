import { useState } from "react";
import { useParams } from "react-router";
import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import type { AppUsageResponse, SeriesData } from "@/api/generated/server-api";
import { ApiState } from "@/components/data/api-state";
import { SeriesBarChart, WritesBarChart } from "@/components/data/charts";
import { DatePeriodPicker } from "@/components/data/date-period-picker";
import { WRU_DESCRIPTION, WruLabel } from "@/components/data/wru-label";
import { PagePanel } from "@/components/data/page-panel";
import { canDrillInto, drillInto, getDefaultDatePeriod } from "@/lib/date-period";
import { TableCell, TableRow } from "@/components/shadcn/ui/table";
import { formatCompact } from "@/lib/format";
import { DashboardStatCards, type DashboardStatCard } from "@/pages/dashboard/dashboard-stat-cards";
import { SectionCard, SectionTable } from "@/pages/apps/section-card";

type BarClickHandler = (entry: Record<string, unknown>) => void;

export function AppUsage() {
    const { slug = "" } = useParams();
    const [period, setPeriod] = useState(getDefaultDatePeriod);

    // Keep the previous charts on screen while the finer-grained period loads, so
    // drilling in reads as a zoom rather than the page blanking out and back in.
    const appUsageQuery = useQuery({ ...api.queries.stats.appUsage(slug, period), placeholderData: keepPreviousData });

    // Every chart is bucketed on the same shared period, so clicking a bucket in
    // any of them drills the whole page from its `t` label.
    const drillFromBar: BarClickHandler | undefined = canDrillInto(period)
        ? (entry) => {
              const next = drillInto(period, entry.t as string);
              if (next) setPeriod(next);
          }
        : undefined;

    return (
        <PagePanel>
            <div className="mb-6 flex items-center justify-end">
                <DatePeriodPicker value={period} onChange={setPeriod} />
            </div>
            <ApiState
                isLoading={appUsageQuery.isPending}
                isError={appUsageQuery.isError}
                errorTitle="Could not load usage"
                onRetry={() => void appUsageQuery.refetch()}
                loadingLabel="Loading usage..."
            >
                {appUsageQuery.data && (
                    <div className="space-y-8">
                        <UsageMetricCards usage={appUsageQuery.data} />
                        <UsageSeriesSection
                            title="Tokens by capability"
                            series={appUsageQuery.data.tokensByCapability}
                            onBarClick={drillFromBar}
                        />
                        <UsageSeriesSection
                            title="Tokens by model"
                            series={appUsageQuery.data.tokensByModel}
                            onBarClick={drillFromBar}
                        />
                        <UsageSeriesSection
                            title="Conversations by channel"
                            series={appUsageQuery.data.conversationsByChannel}
                            onBarClick={drillFromBar}
                        />
                        <CdcWritesSection points={appUsageQuery.data.cdcWrites} onBarClick={drillFromBar} />
                        <TopTablesSection tables={appUsageQuery.data.topTables} />
                        <TopCapabilitiesSection capabilities={appUsageQuery.data.topCapabilities} />
                    </div>
                )}
            </ApiState>
        </PagePanel>
    );
}

function UsageMetricCards({ usage }: { usage: AppUsageResponse }) {
    const { conversations, tokens, cdcWrites } = usage.metrics;
    const cards: DashboardStatCard[] = [
        {
            label: "Conversations",
            value: conversations.value,
            isLoading: false,
            delta: conversations.delta,
            series: conversations.sparkline,
        },
        { label: "Tokens", value: tokens.value, isLoading: false, delta: tokens.delta, series: tokens.sparkline },
        {
            label: "WRU",
            labelInfo: WRU_DESCRIPTION,
            value: cdcWrites.value,
            isLoading: false,
            delta: cdcWrites.delta,
            series: cdcWrites.sparkline,
        },
    ];

    return <DashboardStatCards cards={cards} />;
}

function UsageSeriesSection({
    title,
    series,
    onBarClick,
}: {
    title: string;
    series: SeriesData;
    onBarClick?: BarClickHandler;
}) {
    return (
        <SectionCard title={title}>
            <div className="rounded-lg border p-4">
                {series.keys.length === 0 ? (
                    <p className="py-8 text-center text-sm text-muted-foreground">No data for this period.</p>
                ) : (
                    <SeriesBarChart data={series} onBarClick={onBarClick} />
                )}
            </div>
        </SectionCard>
    );
}

function CdcWritesSection({
    points,
    onBarClick,
}: {
    points: AppUsageResponse["cdcWrites"];
    onBarClick?: BarClickHandler;
}) {
    return (
        <SectionCard title={<WruLabel />}>
            <div className="rounded-lg border p-4">
                <WritesBarChart data={points} xKey="t" onBarClick={onBarClick} />
            </div>
        </SectionCard>
    );
}

function TopTablesSection({ tables }: { tables: AppUsageResponse["topTables"] }) {
    return (
        <SectionCard title="Top tables">
            <SectionTable
                headers={["Table", "WRU"]}
                isEmpty={tables.length === 0}
                emptyMessage="No table activity yet."
            >
                {tables.map((table) => (
                    <TableRow key={table.name}>
                        <TableCell className="font-medium">{table.name}</TableCell>
                        <TableCell className="tabular-nums">{formatCompact(table.writes)}</TableCell>
                    </TableRow>
                ))}
            </SectionTable>
        </SectionCard>
    );
}

function TopCapabilitiesSection({ capabilities }: { capabilities: AppUsageResponse["topCapabilities"] }) {
    return (
        <SectionCard title="Top capabilities">
            <SectionTable
                headers={["Capability", "Invocations", "Avg tokens", "Total tokens"]}
                isEmpty={capabilities.length === 0}
                emptyMessage="No capability usage yet."
            >
                {capabilities.map((capability) => (
                    <TableRow key={capability.name}>
                        <TableCell className="font-medium">{capability.name}</TableCell>
                        <TableCell className="tabular-nums">{formatCompact(capability.invocations)}</TableCell>
                        <TableCell className="tabular-nums">{formatCompact(capability.avgTokens)}</TableCell>
                        <TableCell className="tabular-nums">{formatCompact(capability.totalTokens)}</TableCell>
                    </TableRow>
                ))}
            </SectionTable>
        </SectionCard>
    );
}
