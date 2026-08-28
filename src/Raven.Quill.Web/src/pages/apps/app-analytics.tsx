import { useState } from "react";
import { Text } from "@/components/typography";
import { useParams } from "react-router";
import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import type { AppUsageResponse, SeriesData } from "@/api/generated/server-api";
import { ApiState } from "@/components/data/api-state";
import { SeriesBarChart } from "@/components/data/charts";
import { DatePeriodPicker } from "@/components/data/date-period-picker";
import { ChartSkeleton, DetailGridSkeleton } from "@/components/data/loading-skeletons";
import { PagePanel } from "@/components/data/page-panel";
import { canDrillInto, drillInto, getDefaultDatePeriod, type DatePeriod } from "@/lib/date-period";
import { useAppStartDate } from "@/lib/use-start-date";
import { TableCell, TableRow } from "@/components/shadcn/ui/table";
import { SectionTable } from "@/components/table/section-table";
import { formatCompact } from "@/lib/format";
import { StatCardsSection, type DashboardStatCard } from "@/pages/dashboard/dashboard-stat-cards";
import { SectionCard } from "@/pages/apps/section-card";

type BarClickHandler = (entry: Record<string, unknown>) => void;

export function AppAnalytics() {
    const { slug = "" } = useParams();
    const [period, setPeriod] = useState(getDefaultDatePeriod);
    const appStartDate = useAppStartDate(slug);

    // Keep the previous charts on screen while the finer-grained period loads, so
    // drilling in reads as a zoom rather than the page blanking out and back in.
    const appUsageQuery = useQuery({ ...api.queries.stats.appUsage(slug, period), placeholderData: keepPreviousData });

    // Every chart is bucketed on the same shared period, so clicking a bucket in
    // any of them drills the whole page from its `t` label.
    const drillFromBar: BarClickHandler | undefined = canDrillInto(period)
        ? (entry) => {
              const next = drillInto(period, entry.t as string, appStartDate);
              if (next) setPeriod(next);
          }
        : undefined;

    return (
        <PagePanel>
            <ApiState
                isLoading={appUsageQuery.isPending}
                isError={appUsageQuery.isError}
                errorTitle="Could not load analytics"
                onRetry={() => void appUsageQuery.refetch()}
                loadingLabel="Loading analytics..."
                skeleton={
                    <div className="space-y-8">
                        <DetailGridSkeleton count={4} className="sm:grid-cols-4" />
                        <ChartSkeleton />
                    </div>
                }
            >
                {appUsageQuery.data && (
                    <div className="space-y-8">
                        <AnalyticsMetricCards
                            usage={appUsageQuery.data}
                            period={period}
                            earliest={appStartDate}
                            onPeriodChange={setPeriod}
                        />
                        <AnalyticsSeriesSection
                            title="Tokens by capability"
                            series={appUsageQuery.data.tokensByCapability}
                            onBarClick={drillFromBar}
                        />
                        <AnalyticsSeriesSection
                            title="Tokens by model"
                            series={appUsageQuery.data.tokensByModel}
                            onBarClick={drillFromBar}
                        />
                        <AnalyticsSeriesSection
                            title="Conversations by channel"
                            series={appUsageQuery.data.conversationsByChannel}
                            onBarClick={drillFromBar}
                        />
                        <TopCapabilitiesSection capabilities={appUsageQuery.data.topCapabilities} />
                    </div>
                )}
            </ApiState>
        </PagePanel>
    );
}

function AnalyticsMetricCards({
    usage,
    period,
    earliest,
    onPeriodChange,
}: {
    usage: AppUsageResponse;
    period: DatePeriod;
    earliest: Date | undefined;
    onPeriodChange: (value: DatePeriod) => void;
}) {
    const { conversations, tokens } = usage.metrics;
    const cards: DashboardStatCard[] = [
        {
            label: "Conversations",
            value: conversations.value,
            isLoading: false,
            delta: conversations.delta,
            series: conversations.sparkline,
        },
        { label: "Tokens", value: tokens.value, isLoading: false, delta: tokens.delta, series: tokens.sparkline },
    ];

    return (
        <StatCardsSection
            cards={cards}
            action={<DatePeriodPicker value={period} earliest={earliest} onChange={onPeriodChange} />}
        />
    );
}

function AnalyticsSeriesSection({
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
                    <Text variant="muted" className="py-8 text-center">
                        No data for this period.
                    </Text>
                ) : (
                    <SeriesBarChart data={series} onBarClick={onBarClick} />
                )}
            </div>
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
