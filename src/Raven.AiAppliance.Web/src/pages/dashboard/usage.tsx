import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { ChevronLeft, ChevronRight } from "lucide-react";
import { api } from "@/api/api";
import type { QuillApplicationUsage, QuillPeriodUsage } from "@/api/generated/server-api";
import { ApiState } from "@/components/data/api-state";
import { WritesBarChart } from "@/components/data/charts";
import { Button } from "@/components/shadcn/ui/button";
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/shadcn/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/shadcn/ui/table";
import { formatCompact } from "@/lib/format";

export function DashboardUsage() {
    const now = new Date();
    const [{ year, month }, setSelectedMonth] = useState({ year: now.getFullYear(), month: now.getMonth() + 1 });

    const usageQuery = useQuery(api.queries.settings.usage(year, month));

    const totalUsage = usageQuery.data?.byPeriod?.reduce((sum, period) => sum + period.usage, 0);

    const isAtCurrentMonth = year === now.getFullYear() && month === now.getMonth() + 1;
    const monthLabel = new Date(year, month - 1).toLocaleString("en-US", { month: "long", year: "numeric" });

    const goToMonth = (offset: number) =>
        setSelectedMonth(({ year: y, month: m }) => {
            const next = new Date(y, m - 1 + offset);
            return { year: next.getFullYear(), month: next.getMonth() + 1 };
        });

    return (
        <div className="space-y-5">
            <div className="flex items-center justify-between gap-3">
                <h1 className="text-2xl font-semibold tracking-tight">Usage</h1>
                <div className="flex items-center gap-1 rounded-lg border p-1">
                    <Button variant="ghost" size="icon-sm" aria-label="Previous month" onClick={() => goToMonth(-1)}>
                        <ChevronLeft aria-hidden="true" />
                    </Button>
                    <span className="min-w-28 text-center text-sm font-medium">{monthLabel}</span>
                    <Button
                        variant="ghost"
                        size="icon-sm"
                        aria-label="Next month"
                        disabled={isAtCurrentMonth}
                        onClick={() => goToMonth(1)}
                    >
                        <ChevronRight aria-hidden="true" />
                    </Button>
                </div>
            </div>

            <Card>
                <CardHeader>
                    <CardTitle>Writes this month</CardTitle>
                    <CardDescription>{monthLabel}</CardDescription>
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
                            <WritesBarChart data={toChartData(usageQuery.data.byPeriod ?? [])} xKey="label" />
                        )}
                    </ApiState>
                </CardContent>
            </Card>

            <Card>
                <CardHeader>
                    <CardTitle>Usage per app</CardTitle>
                    <CardDescription>Totals for {monthLabel}.</CardDescription>
                </CardHeader>
                <CardContent className="px-0">
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

const chartDayFormatter = new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric" });

function toChartData(byPeriod: QuillPeriodUsage[]) {
    return byPeriod.map((period) => ({
        label: chartDayFormatter.format(new Date(period.from)),
        writes: period.usage,
    }));
}

function PerAppUsageTable({ apps }: { apps: QuillApplicationUsage[] }) {
    return (
        <Table>
            <TableHeader>
                <TableRow className="hover:bg-transparent">
                    <TableHead className="w-full pl-4 text-xs font-medium text-muted-foreground">Name</TableHead>
                    <TableHead className="pr-4 text-right text-xs font-medium text-muted-foreground">Writes</TableHead>
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
