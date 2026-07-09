import { useState } from "react";
import { useParams } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { endOfMonth } from "date-fns";
import { api } from "@/api/api";
import type { AppUsageResponse, SeriesData } from "@/api/generated/server-api";
import { ApiState } from "@/components/data/api-state";
import { SeriesBarChart, WritesBarChart } from "@/components/data/charts";
import { MonthPicker } from "@/components/data/month-picker";
import { PagePanel } from "@/components/data/page-panel";
import { getCurrentMonth, type MonthSelection } from "@/lib/month";
import { TableCell, TableRow } from "@/components/shadcn/ui/table";
import { formatCompact } from "@/lib/format";
import { formatDateTime } from "@/lib/utils";
import { DashboardStatCards, type DashboardStatCard } from "@/pages/dashboard/dashboard-stat-cards";
import { SectionCard, SectionTable } from "@/pages/apps/section-card";

// The endpoint accepts ISO start/end query strings and parses them as UTC; send the full inclusive span of the picked month
function toApiRange({ year, month }: MonthSelection) {
    const monthStart = new Date(year, month - 1, 1);
    return { start: monthStart.toISOString(), end: endOfMonth(monthStart).toISOString() };
}

export function AppUsage() {
    const { slug = "" } = useParams();
    const [selectedMonth, setSelectedMonth] = useState<MonthSelection>(getCurrentMonth);
    const appUsageQuery = useQuery(api.queries.stats.appUsage(slug, toApiRange(selectedMonth)));

    return (
        <PagePanel>
            <div className="mb-6 flex items-center justify-end">
                <MonthPicker value={selectedMonth} onChange={setSelectedMonth} />
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
                        />
                        <UsageSeriesSection title="Tokens by model" series={appUsageQuery.data.tokensByModel} />
                        <UsageSeriesSection
                            title="Conversations by channel"
                            series={appUsageQuery.data.conversationsByChannel}
                        />
                        <CdcWritesSection points={appUsageQuery.data.cdcWrites} />
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
            label: "CDC writes",
            value: cdcWrites.value,
            isLoading: false,
            delta: cdcWrites.delta,
            series: cdcWrites.sparkline,
        },
    ];

    return <DashboardStatCards cards={cards} />;
}

function UsageSeriesSection({ title, series }: { title: string; series: SeriesData }) {
    return (
        <SectionCard title={title}>
            <div className="rounded-lg border p-4">
                {series.keys.length === 0 ? (
                    <p className="py-8 text-center text-sm text-muted-foreground">No data for this period.</p>
                ) : (
                    <SeriesBarChart data={series} />
                )}
            </div>
        </SectionCard>
    );
}

function CdcWritesSection({ points }: { points: AppUsageResponse["cdcWrites"] }) {
    return (
        <SectionCard title="CDC writes">
            <div className="rounded-lg border p-4">
                <WritesBarChart data={points} xKey="t" />
            </div>
        </SectionCard>
    );
}

function TopTablesSection({ tables }: { tables: AppUsageResponse["topTables"] }) {
    return (
        <SectionCard title="Top tables">
            <SectionTable
                headers={["Table", "Writes", "Lag", "Last write"]}
                isEmpty={tables.length === 0}
                emptyMessage="No table activity yet."
            >
                {tables.map((table) => (
                    <TableRow key={table.name}>
                        <TableCell className="font-medium">{table.name}</TableCell>
                        <TableCell className="tabular-nums">{formatCompact(table.writes)}</TableCell>
                        <TableCell className="text-muted-foreground tabular-nums">{table.lagSeconds}s</TableCell>
                        <TableCell className="whitespace-nowrap text-muted-foreground">
                            {formatDateTime(table.lastWriteAt)}
                        </TableCell>
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
