import { useParams } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { Bar, BarChart, CartesianGrid, XAxis, YAxis } from "recharts";
import { api } from "@/api/api";
import type { AppUsageResponse } from "@/api/generated/server-api";
import { ApiState } from "@/components/data/api-state";
import { PagePanel } from "@/components/data/page-panel";
import { ChartContainer, ChartTooltip, ChartTooltipContent, type ChartConfig } from "@/components/shadcn/ui/chart";
import { TableCell, TableRow } from "@/components/shadcn/ui/table";
import { formatCompact } from "@/lib/format";
import { formatDateTime } from "@/lib/utils";
import { DashboardStatCards, type DashboardStatCard } from "@/pages/dashboard/dashboard-stat-cards";
import { SectionCard, SectionTable } from "@/pages/apps/section-card";

const currencyFormatter = new Intl.NumberFormat("en-US", { style: "currency", currency: "USD" });

function formatCurrency(value: number) {
    return currencyFormatter.format(value);
}

export function AppUsage() {
    const { slug = "" } = useParams();
    const appUsageQuery = useQuery(api.queries.stats.appUsage(slug));

    return (
        <PagePanel>
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
    const { conversations, tokens, cost, cdcWrites } = usage.metrics;
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
            label: "Cost",
            value: cost.value,
            valueLabel: formatCurrency(cost.value),
            isLoading: false,
            delta: cost.delta,
            series: cost.sparkline,
        },
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

const cdcChartConfig = {
    writes: { label: "Writes", color: "var(--chart-1)" },
} satisfies ChartConfig;

function CdcWritesSection({ points }: { points: AppUsageResponse["cdcWrites"] }) {
    return (
        <SectionCard title="CDC writes">
            <div className="rounded-lg border p-4">
                <ChartContainer config={cdcChartConfig} className="aspect-auto h-56 w-full">
                    <BarChart accessibilityLayer data={points} margin={{ top: 8, right: 0, bottom: 0, left: 0 }}>
                        <CartesianGrid vertical={false} />
                        <XAxis dataKey="t" tickLine={false} axisLine={false} tickMargin={8} interval={2} />
                        <YAxis hide domain={[0, "dataMax"]} />
                        <ChartTooltip cursor={false} content={<ChartTooltipContent />} />
                        <Bar dataKey="writes" fill="var(--color-writes)" radius={[4, 4, 0, 0]} />
                    </BarChart>
                </ChartContainer>
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
                headers={["Capability", "Invocations", "Avg tokens", "Total tokens", "Cost"]}
                isEmpty={capabilities.length === 0}
                emptyMessage="No capability usage yet."
            >
                {capabilities.map((capability) => (
                    <TableRow key={capability.name}>
                        <TableCell className="font-medium">{capability.name}</TableCell>
                        <TableCell className="tabular-nums">{formatCompact(capability.invocations)}</TableCell>
                        <TableCell className="tabular-nums">{formatCompact(capability.avgTokens)}</TableCell>
                        <TableCell className="tabular-nums">{formatCompact(capability.totalTokens)}</TableCell>
                        <TableCell className="tabular-nums">{formatCurrency(capability.cost)}</TableCell>
                    </TableRow>
                ))}
            </SectionTable>
        </SectionCard>
    );
}
