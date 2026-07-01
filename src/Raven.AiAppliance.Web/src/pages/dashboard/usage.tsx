import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { ChevronLeft, ChevronRight, RefreshCw } from "lucide-react";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
import { WritesBarChart } from "@/components/data/charts";
import { Button } from "@/components/shadcn/ui/button";
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/shadcn/ui/card";
import { Progress } from "@/components/shadcn/ui/progress";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/shadcn/ui/table";
import { formatCompact } from "@/lib/format";
import { formatRelativeTime } from "@/lib/utils";

export function DashboardUsage() {
    const now = new Date();
    const [{ year, month }, setSelectedMonth] = useState({ year: now.getFullYear(), month: now.getMonth() + 1 });

    const usageQuery = useQuery(api.queries.settings.usage(year, month));
    const tokensByAppQuery = useQuery(api.queries.stats.tokensByApp());

    // Anchor the "Refreshed …" label to when the server aggregated the data
    // (refreshedMinutesAgo), not when the browser fetched it.
    const refreshedAt = tokensByAppQuery.data
        ? tokensByAppQuery.dataUpdatedAt - tokensByAppQuery.data.refreshedMinutesAgo * 60_000
        : undefined;

    const isAtCurrentMonth = year === now.getFullYear() && month === now.getMonth() + 1;
    const monthLabel =
        usageQuery.data?.monthLabel ??
        new Date(year, month - 1).toLocaleString("en-US", { month: "long", year: "numeric" });

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
                    <CardDescription>
                        {usageQuery.data
                            ? `Monthly quota resets on ${usageQuery.data.quotaResetsOn}`
                            : "Loading monthly writes…"}
                    </CardDescription>
                    {usageQuery.data && (
                        <CardAction className="text-right">
                            <div className="text-2xl font-semibold">
                                {formatCompact(usageQuery.data.monthlyUsed)}
                                <span className="ml-1 text-sm font-normal text-muted-foreground">
                                    of {formatCompact(usageQuery.data.monthlyQuota)}
                                </span>
                            </div>
                            <Progress
                                className="mt-2 ml-auto h-1.5 w-56"
                                value={
                                    usageQuery.data.monthlyQuota > 0
                                        ? Math.min(
                                              100,
                                              (usageQuery.data.monthlyUsed / usageQuery.data.monthlyQuota) * 100,
                                          )
                                        : 0
                                }
                            />
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
                        {usageQuery.data && <WritesBarChart data={usageQuery.data.days} xKey="label" />}
                    </ApiState>
                </CardContent>
            </Card>

            <Card>
                <CardHeader>
                    <CardTitle>Token usage per app</CardTitle>
                    <CardDescription>
                        All-time totals.
                        {refreshedAt !== undefined && ` Refreshed ${formatRelativeTime(refreshedAt)}.`}
                    </CardDescription>
                    <CardAction>
                        <Button
                            variant="outline"
                            size="sm"
                            onClick={() => tokensByAppQuery.refetch()}
                            disabled={tokensByAppQuery.isFetching}
                        >
                            <RefreshCw aria-hidden="true" />
                            Refresh
                        </Button>
                    </CardAction>
                </CardHeader>
                <CardContent className="px-0">
                    <ApiState
                        isLoading={tokensByAppQuery.isPending}
                        isError={tokensByAppQuery.isError}
                        errorTitle="Could not load per-app usage"
                        onRetry={() => tokensByAppQuery.refetch()}
                        loadingLabel="Loading apps…"
                    >
                        {tokensByAppQuery.data && <PerAppUsageTable apps={tokensByAppQuery.data.apps} />}
                    </ApiState>
                </CardContent>
            </Card>
        </div>
    );
}

function PerAppUsageTable({ apps }: { apps: { slug: string; tokens: number }[] }) {
    const maxTokens = Math.max(1, ...apps.map((app) => app.tokens));

    return (
        <Table>
            <TableHeader>
                <TableRow className="hover:bg-transparent">
                    <TableHead className="w-full pl-4 text-xs font-medium text-muted-foreground">Name</TableHead>
                    <TableHead className="pr-4 text-right text-xs font-medium text-muted-foreground">Tokens</TableHead>
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
                        <TableRow key={app.slug}>
                            <TableCell className="py-3 pl-4 font-medium">{app.slug}</TableCell>
                            <TableCell className="py-3 pr-4">
                                <div className="ml-auto flex w-64 max-w-full items-center gap-3">
                                    <Progress className="h-1.5 flex-1" value={(app.tokens / maxTokens) * 100} />
                                    <span className="w-16 text-right text-muted-foreground tabular-nums">
                                        {app.tokens.toLocaleString()}
                                    </span>
                                </div>
                            </TableCell>
                        </TableRow>
                    ))
                )}
            </TableBody>
        </Table>
    );
}
