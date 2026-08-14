import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import type { CdcPerformanceResponse } from "@/api/generated/server-api";
import { ApiState } from "@/components/data/api-state";
import { Button } from "@/components/shadcn/ui/button";
import { Card, CardContent } from "@/components/shadcn/ui/card";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/shadcn/ui/tooltip";
import { formatCompact } from "@/lib/format";
import { cn, formatDateTime, formatRelativeTime } from "@/lib/utils";
import { CdcErrorsSheet } from "@/pages/apps/cdc-errors-sheet";
import { summarizeCdcErrors, type CdcErrorsSummary } from "@/pages/apps/cdc-errors-summary";
import { SectionCard } from "@/pages/apps/section-card";
import { SyncActivityDots } from "@/pages/apps/sync-activity-dots";
import { hasErroredSinceLastSync, toSyncStatus, type SyncStatus } from "@/pages/apps/sync-status";
import { SyncStatusBadge } from "@/pages/apps/sync-status-badge";

/**
 * One place on the overview for both readings of the app's sync: the status right now, and the
 * error log that outlives it. They legitimately disagree - an error stays in the log long after
 * the batch that raised it fell out of the sink's window - so each is shown with the wording that
 * says which is which, rather than as a single alarming number.
 */
export function SyncHealthCard({ slug }: { slug: string }) {
    const performanceQuery = useQuery(api.queries.apps.cdcPerformance(slug));
    const errorsQuery = useQuery(api.queries.apps.cdcErrors(slug));

    const performance = performanceQuery.data;
    const errors = summarizeCdcErrors(errorsQuery.data);
    const reportedStatus = performance ? toSyncStatus(performance.status) : null;
    const isFailing = isSyncFailing(performance, reportedStatus, errors);
    // Say what the card concluded, so the badge cannot read "idle" beside errors marked as current.
    const status = isFailing ? "error" : reportedStatus;

    return (
        <SectionCard
            title={
                <span className="flex items-center gap-2">
                    Data sync
                    {status && <SyncStatusBadge status={status} />}
                </span>
            }
            action={
                errors.count > 0 && (
                    <CdcErrorsSheet
                        slug={slug}
                        trigger={
                            <Button variant={isFailing ? "destructive-outline" : "outline"} size="sm">
                                View errors
                            </Button>
                        }
                    />
                )
            }
        >
            <ApiState
                isLoading={performanceQuery.isPending}
                isError={performanceQuery.isError}
                errorTitle="Could not load sync status"
                onRetry={() => void performanceQuery.refetch()}
                loadingLabel="Loading sync status..."
            >
                {performance && (
                    <Card className={cn(isFailing && "border-destructive")}>
                        {/* Flow the readings side by side at their natural width. A grid would stretch
                            two items across the full card, leaving a chasm between them on a wide screen. */}
                        <CardContent className="flex flex-wrap items-center gap-x-6 gap-y-4">
                            <SyncDetail
                                label="Last sync"
                                value={describeLastSync(performance.lagSeconds)}
                                exactAt={performance.lastSyncAt}
                            />
                            <SyncDetail
                                label="Errors"
                                value={describeErrors(errors)}
                                exactAt={errors.latestAt}
                                isDestructive={isFailing}
                            />
                            {/* Right-aligned: it fills the width the two readings leave empty
                                rather than adding a row of its own. */}
                            <div className="ml-auto">
                                <SyncActivityDots batches={performance.recentBatches} />
                            </div>
                        </CardContent>
                    </Card>
                )}
            </ApiState>
        </SectionCard>
    );
}

/**
 * Only a sink that has not recovered earns the destructive treatment. A non-empty log on its own
 * does not: it outlives the failures it records, so styling it red would mark an app as broken
 * forever over a transient error it already synced past. The reported status is not enough on its
 * own either - it goes blind to failures that never produced a batch - so the two instants on the
 * card settle it.
 *
 * Lives outside the component because it reads the clock, which a render must not do directly.
 */
function isSyncFailing(
    performance: CdcPerformanceResponse | undefined,
    reportedStatus: SyncStatus | null,
    errors: CdcErrorsSummary,
): boolean {
    // A disabled task is not failing, it is switched off.
    if (performance === undefined || reportedStatus === "disabled") {
        return false;
    }

    return reportedStatus === "error" || hasErroredSinceLastSync(errors.latestAt, performance.lagSeconds, Date.now());
}

// lagSeconds is measured from the durable last-activity timestamp, so it still reads correctly
// after a restart empties the in-memory batch window - unlike lastSyncAt, which comes back null.
// It is turned back into an instant so both readings on the card share one relative-time
// vocabulary; two of them a line apart read as two different scales.
function describeLastSync(lagSeconds: number | null): string {
    return lagSeconds === null ? "No sync recorded yet" : formatRelativeTime(Date.now() - lagSeconds * 1000);
}

// Dating the newest error is what keeps the log from reading as "happening right now" when the
// status beside it says otherwise.
function describeErrors({ count, latestAt }: CdcErrorsSummary): string {
    if (count === 0) {
        return "None recorded";
    }

    const recorded = `${formatCompact(count)} recorded`;
    return latestAt ? `${recorded}, latest ${formatRelativeTime(latestAt)}` : recorded;
}

/**
 * `exactAt` is the precise instant behind a relative reading, shown on hover so a lag can be
 * matched against a server log. It is only passed where the server gave an actual timestamp:
 * "Last sync" reconstructs its instant from lagSeconds, which carries clock skew and request
 * latency, so that reading stays relative-only whenever lastSyncAt came back null.
 */
function SyncDetail({
    label,
    value,
    exactAt,
    isDestructive = false,
}: {
    label: string;
    value: string;
    exactAt?: string | null;
    isDestructive?: boolean;
}) {
    const valueClassName = cn("text-sm font-medium", isDestructive && "text-destructive");

    if (!exactAt) {
        return (
            <div className="space-y-0.5">
                <div className="text-xs text-muted-foreground">{label}</div>
                <div className={valueClassName}>{value}</div>
            </div>
        );
    }

    return (
        <div className="space-y-0.5">
            <div className="text-xs text-muted-foreground">{label}</div>
            <TooltipProvider>
                <Tooltip>
                    <TooltipTrigger asChild>
                        {/* Focusable: a relative reading is useless to a keyboard user if the exact
                            instant behind it can only be reached with a pointer. */}
                        <span
                            tabIndex={0}
                            className={cn(
                                valueClassName,
                                "block w-fit rounded-sm focus-visible:ring-[3px] focus-visible:ring-ring/50 focus-visible:outline-none",
                            )}
                        >
                            {value}
                        </span>
                    </TooltipTrigger>
                    <TooltipContent>{formatDateTime(exactAt)}</TooltipContent>
                </Tooltip>
            </TooltipProvider>
        </div>
    );
}
