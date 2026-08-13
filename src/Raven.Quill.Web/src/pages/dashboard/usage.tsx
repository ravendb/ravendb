import { useState } from "react";
import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { format } from "date-fns";
import { api } from "@/api/api";
import type { QuillApplicationUsage, QuillPeriodUsage } from "@/api/generated/server-api";
import { ApiState } from "@/components/data/api-state";
import { WritesBarChart } from "@/components/data/charts";
import { DatePeriodPicker } from "@/components/data/date-period-picker";
import { WruLabel } from "@/components/data/wru-label";
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/shadcn/ui/card";
import { canDrillInto, drillInto, formatPeriodLabel, getDefaultDatePeriod, type DatePeriod } from "@/lib/date-period";
import { useSetupStartDate } from "@/lib/use-start-date";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/shadcn/ui/table";
import { formatCompact } from "@/lib/format";

export function DashboardUsage() {
    const [period, setPeriod] = useState(getDefaultDatePeriod);
    const setupStartDate = useSetupStartDate();

    // Keep the previous chart on screen while the finer-grained period loads, so
    // drilling in reads as a zoom rather than the chart blanking out and back in.
    const usageQuery = useQuery({ ...api.queries.settings.usage(period), placeholderData: keepPreviousData });

    const totalUsage = usageQuery.data?.byPeriod?.reduce((sum, bucket) => sum + bucket.usage, 0);

    const periodLabel = formatPeriodLabel(period);
    const chartData = toChartData(usageQuery.data?.byPeriod ?? [], period);

    const drillFromBar = (entry: Record<string, unknown>) => {
        const next = drillInto(period, entry.from as string, setupStartDate);
        if (next) setPeriod(next);
    };

    return (
        <div className="space-y-5">
            <div className="flex items-center justify-between gap-3">
                <h1 className="text-2xl font-semibold tracking-tight">Usage</h1>
                <DatePeriodPicker value={period} earliest={setupStartDate} onChange={setPeriod} />
            </div>

            <Card>
                <CardHeader>
                    <CardTitle>
                        <WruLabel />
                    </CardTitle>
                    <CardDescription>{periodLabel}</CardDescription>
                    {totalUsage !== undefined && (
                        <CardAction className="text-right">
                            <div className="text-2xl font-semibold">
                                {formatCompact(totalUsage)}
                                <span className="ml-1 text-sm font-normal text-muted-foreground">total</span>
                            </div>
                        </CardAction>
                    )}
                </CardHeader>
                <CardContent>
                    <ApiState
                        isLoading={usageQuery.isPending}
                        isError={usageQuery.isError}
                        errorTitle="Could not load usage"
                        onRetry={() => usageQuery.refetch()}
                        loadingLabel="Loading chart…"
                    >
                        {usageQuery.data && (
                            <WritesBarChart
                                data={chartData}
                                xKey="label"
                                onBarClick={canDrillInto(period) ? drillFromBar : undefined}
                            />
                        )}
                    </ApiState>
                </CardContent>
            </Card>

            <Card>
                <CardHeader>
                    <CardTitle>Usage per app</CardTitle>
                    <CardDescription>Totals for {periodLabel}.</CardDescription>
                </CardHeader>
                <CardContent className={usageQuery.data ? "px-0" : undefined}>
                    <ApiState
                        isLoading={usageQuery.isPending}
                        isError={usageQuery.isError}
                        errorTitle="Could not load per-app usage"
                        onRetry={() => usageQuery.refetch()}
                        loadingLabel="Loading apps…"
                    >
                        {usageQuery.data && <PerAppUsageTable apps={usageQuery.data.perApplication ?? []} />}
                    </ApiState>
                </CardContent>
            </Card>
        </div>
    );
}

// Bucket labels match the selected granularity: months of a year, days of a
// month, or hours of a day.
function bucketLabelFormat(period: DatePeriod): string {
    if (period.month === null) return "MMM";
    if (period.day === null) return "MMM d";
    return "h a";
}

function toChartData(byPeriod: QuillPeriodUsage[], period: DatePeriod) {
    const now = new Date();
    const labelFormat = bucketLabelFormat(period);
    return byPeriod
        .filter((bucket) => new Date(bucket.from) <= now)
        .map((bucket) => ({
            label: format(new Date(bucket.from), labelFormat),
            writes: bucket.usage,
            from: bucket.from,
        }));
}

function PerAppUsageTable({ apps }: { apps: QuillApplicationUsage[] }) {
    return (
        <Table>
            <TableHeader>
                <TableRow className="hover:bg-transparent">
                    <TableHead className="w-full pl-4 text-xs font-medium text-muted-foreground">Name</TableHead>
                    <TableHead className="pr-4 text-right text-xs font-medium text-muted-foreground">
                        <WruLabel />
                    </TableHead>
                </TableRow>
            </TableHeader>
            <TableBody>
                {apps.length === 0 ? (
                    <TableRow className="hover:bg-transparent">
                        <TableCell colSpan={2} className="h-20 text-center text-muted-foreground">
                            No usage tracked yet.
                        </TableCell>
                    </TableRow>
                ) : (
                    apps.map((app) => (
                        <TableRow key={`${app.topologyId}/${app.applicationName}`}>
                            <TableCell className="py-3 pl-4 font-medium">{app.applicationName}</TableCell>
                            <TableCell className="py-3 pr-4">
                                <span className="w-16 text-right text-muted-foreground tabular-nums">
                                    {app.usage.toLocaleString()}
                                </span>
                            </TableCell>
                        </TableRow>
                    ))
                )}
            </TableBody>
        </Table>
    );
}
