import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { ChevronLeft, ChevronRight, RefreshCw } from "lucide-react";
import { api } from "@/api/api";
import type { DayWrites } from "@/api/generated/server-api";
import { ApiState } from "@/components/data/api-state";
import { Button } from "@/components/shadcn/ui/button";
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/shadcn/ui/card";
import { Progress } from "@/components/shadcn/ui/progress";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/shadcn/ui/table";

const compactFormatter = new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 1 });

function formatCompact(value: number) {
    return compactFormatter.format(value);
}

function formatTimeAgo(timestamp: number) {
    const minutes = Math.round((Date.now() - timestamp) / 60_000);
    if (minutes < 1) {
        return "just now";
    }
    if (minutes < 60) {
        return `${minutes} min ago`;
    }
    return `${Math.round(minutes / 60)} h ago`;
}

export function DashboardUsage() {
    const now = new Date();
    const [{ year, month }, setSelectedMonth] = useState({ year: now.getFullYear(), month: now.getMonth() + 1 });

    const usageQuery = useQuery(api.queries.settings.usage(year, month));
    const tokensByAppQuery = useQuery(api.queries.stats.tokensByApp());

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
                                value={Math.min(
                                    100,
                                    (usageQuery.data.monthlyUsed / usageQuery.data.monthlyQuota) * 100,
                                )}
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
                        {usageQuery.data && <WritesChart days={usageQuery.data.days} />}
                    </ApiState>
                </CardContent>
            </Card>

            <Card>
                <CardHeader>
                    <CardTitle>Writes usage per app</CardTitle>
                    <CardDescription>
                        Tracked for this period.
                        {tokensByAppQuery.dataUpdatedAt > 0 &&
                            ` Refreshed ${formatTimeAgo(tokensByAppQuery.dataUpdatedAt)}.`}
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

function WritesChart({ days }: { days: DayWrites[] }) {
    const maxWrites = Math.max(1, ...days.map((day) => day.writes));

    return (
        <div className="flex h-56 flex-col">
            <div className="relative flex flex-1 items-end gap-1 border-b">
                <div className="pointer-events-none absolute inset-0 flex flex-col justify-between">
                    {[0, 1, 2, 3].map((line) => (
                        <div key={line} className="border-t border-border/50" />
                    ))}
                </div>
                {days.map((day) => (
                    <div
                        key={day.date}
                        className="relative flex-1 rounded-t-sm bg-primary/70 transition-colors hover:bg-primary"
                        style={{ height: `${(day.writes / maxWrites) * 100}%` }}
                        title={`${day.label}: ${day.writes.toLocaleString()} writes`}
                    />
                ))}
            </div>
            <div className="flex gap-1 pt-2">
                {days.map((day) => {
                    const dayOfMonth = Number(day.date.slice(8, 10));
                    return (
                        <div key={day.date} className="flex-1 text-center text-xs text-muted-foreground">
                            {dayOfMonth % 3 === 1 ? day.label : ""}
                        </div>
                    );
                })}
            </div>
        </div>
    );
}

function PerAppUsageTable({ apps }: { apps: { slug: string; tokens: number }[] }) {
    const maxTokens = Math.max(1, ...apps.map((app) => app.tokens));

    return (
        <Table>
            <TableHeader>
                <TableRow className="hover:bg-transparent">
                    <TableHead className="text-xs font-medium text-muted-foreground">Name</TableHead>
                    <TableHead className="text-xs font-medium text-muted-foreground">Usage</TableHead>
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
                            <TableCell className="font-medium">{app.slug}</TableCell>
                            <TableCell>
                                <div className="flex items-center gap-4">
                                    <Progress
                                        className="h-1.5 max-w-64 flex-1"
                                        value={(app.tokens / maxTokens) * 100}
                                    />
                                    <span className="w-20 text-right tabular-nums">{app.tokens.toLocaleString()}</span>
                                </div>
                            </TableCell>
                        </TableRow>
                    ))
                )}
            </TableBody>
        </Table>
    );
}
