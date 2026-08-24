import { Fragment, useState } from "react";
import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { format } from "date-fns";
import { ChevronRight } from "lucide-react";
import { api } from "@/api/api";
import type { QuillApplicationUsage, QuillPeriodUsage } from "@/api/generated/server-api";
import { ApiState } from "@/components/data/api-state";
import { WritesBarChart } from "@/components/data/charts";
import { DatePeriodPicker } from "@/components/data/date-period-picker";
import { InfoHint } from "@/components/data/info-hint";
import { WruLabel } from "@/components/data/wru-label";
import { Badge } from "@/components/shadcn/ui/badge";
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/shadcn/ui/card";
import { canDrillInto, drillInto, formatPeriodLabel, getDefaultDatePeriod, type DatePeriod } from "@/lib/date-period";
import { useSetupStartDate } from "@/lib/use-start-date";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/shadcn/ui/table";
import { formatCompact } from "@/lib/format";
import { cn } from "@/lib/utils";
import { rowKey, SYSTEM_GROUP_DESCRIPTION, toUsageGroups, type UsageGroup } from "@/pages/dashboard/usage-groups";

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

// The name column reserves the chevron's width on every row, expandable or not, so all the names
// line up in one column and the chevrons sit in a gutter of their own.
const CHEVRON_GUTTER = "size-3.5 shrink-0";

// Inter's font box sits high in a text-sm line box - more of the leading falls below the letters
// than above - so an icon centred on that line box reads a hair low against them. `leading-none`
// collapses the line box onto the font box, which centres the letters and the chevron on the same
// axis, and the fixed height keeps every row as tall as it was.
const NAME_ROW = "flex h-5 items-center gap-1.5 leading-none";

// Where the names land: pl-4 + the gutter + the gap. Members of an expanded group indent to it, so
// they read as sitting under the name they belong to.
const NAME_INDENT = "pl-9";

function UsageCell({ usage, className }: { usage: number; className?: string }) {
    return (
        <TableCell className={cn("py-3 pr-4 text-right text-muted-foreground tabular-nums", className)}>
            {usage.toLocaleString()}
        </TableCell>
    );
}

// The group's own row: its shared name (or "System"), how many rows it stands for, and their
// combined usage. The whole row toggles it; the button carries the state and the keyboard, and lets
// its click bubble to the row so one handler serves both. Groups standing for a single app have
// nothing to expand and render as the plain row they always were.
function GroupRow({ group, isOpen, onToggle }: { group: UsageGroup; isOpen: boolean; onToggle: () => void }) {
    return (
        <TableRow
            onClick={group.isExpandable ? onToggle : undefined}
            className={cn(group.isExpandable && "cursor-pointer")}
        >
            <TableCell className="py-3 pl-4 font-medium">
                <span className={NAME_ROW}>
                    {group.isExpandable ? (
                        <>
                            <button
                                type="button"
                                aria-expanded={isOpen}
                                className={cn(
                                    "-mx-0.5 flex items-center gap-1.5 rounded-sm px-0.5",
                                    "focus-visible:ring-[3px] focus-visible:ring-ring/50 focus-visible:outline-none",
                                )}
                            >
                                <ChevronRight
                                    aria-hidden="true"
                                    className={cn(
                                        CHEVRON_GUTTER,
                                        "text-muted-foreground transition-transform",
                                        isOpen && "rotate-90",
                                    )}
                                />
                                {group.label}
                            </button>
                            {group.isSystem && (
                                // The hint explains the group; it doesn't toggle it.
                                <span onClick={(event) => event.stopPropagation()}>
                                    <InfoHint content={SYSTEM_GROUP_DESCRIPTION} />
                                </span>
                            )}
                            <Badge variant="secondary" className="tabular-nums">
                                {group.rows.length}
                            </Badge>
                        </>
                    ) : (
                        <>
                            <span aria-hidden="true" className={CHEVRON_GUTTER} />
                            {group.label}
                        </>
                    )}
                </span>
            </TableCell>
            <UsageCell usage={group.usage} />
        </TableRow>
    );
}

// One row per database behind an expanded group, labelled by the topology id - the only thing that
// tells apart rows that share a name.
function GroupMemberRow({ usage, topologyId }: { usage: number; topologyId: string }) {
    return (
        <TableRow className="hover:bg-transparent">
            <TableCell className={cn("py-2 font-mono text-xs text-muted-foreground", NAME_INDENT)}>
                {topologyId}
            </TableCell>
            <UsageCell usage={usage} className="py-2" />
        </TableRow>
    );
}

function PerAppUsageTable({ apps }: { apps: QuillApplicationUsage[] }) {
    const [openGroups, setOpenGroups] = useState<ReadonlySet<string>>(new Set());
    const groups = toUsageGroups(apps);

    const toggle = (key: string) =>
        setOpenGroups((open) => {
            const next = new Set(open);
            if (next.has(key)) next.delete(key);
            else next.add(key);
            return next;
        });

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
                {groups.length === 0 ? (
                    <TableRow className="hover:bg-transparent">
                        <TableCell colSpan={2} className="h-20 text-center text-muted-foreground">
                            No usage tracked yet.
                        </TableCell>
                    </TableRow>
                ) : (
                    groups.map((group) => {
                        const isOpen = openGroups.has(group.key);
                        return (
                            <Fragment key={group.key}>
                                <GroupRow group={group} isOpen={isOpen} onToggle={() => toggle(group.key)} />
                                {isOpen &&
                                    group.rows.map((row) => (
                                        <GroupMemberRow
                                            key={rowKey(row)}
                                            usage={row.usage}
                                            topologyId={row.topologyId}
                                        />
                                    ))}
                            </Fragment>
                        );
                    })
                )}
            </TableBody>
        </Table>
    );
}
