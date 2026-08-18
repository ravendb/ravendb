import { useId, useLayoutEffect, useRef, useState, type ReactNode, type UIEvent } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import { ChevronRight } from "lucide-react";
import { StatusIndicator, type StatusTone } from "@/components/data/status-indicator";
import { Label } from "@/components/shadcn/ui/label";
import { Switch } from "@/components/shadcn/ui/switch";
import { formatCompact } from "@/lib/format";
import { cn } from "@/lib/utils";
import type { CdcLiveBatch, CdcLiveBatchState } from "@/pages/apps/use-cdc-live-performance";

// The h-10 header button plus the 1px row separator, so an unmeasured collapsed row already
// estimates to its real height and the total size barely moves once measuring catches up.
const COLLAPSED_ROW_HEIGHT_IN_PX = 41;
const OVERSCAN = 12;
// Slack around the bottom edge so a resting scroll position still counts as "at the latest".
const AT_LATEST_THRESHOLD_IN_PX = 24;
// How far the view has to travel back up before it reads as the user leaving the live tail.
const SCROLL_UP_SLACK_IN_PX = 8;

export function CdcBatchLog({ batches }: { batches: CdcLiveBatch[] }) {
    if (batches.length === 0) {
        return (
            <div className="rounded-lg border">
                <p className="py-12 text-center text-sm text-muted-foreground">No batches yet.</p>
            </div>
        );
    }

    return <CdcBatchList batches={batches} />;
}

function CdcBatchList({ batches }: { batches: CdcLiveBatch[] }) {
    // The virtualizer returns fresh functions on every render, so memoizing this component would
    // freeze the visible window on its first value. The rows below stay compiled.
    "use no memo";
    const followId = useId();
    const scrollRef = useRef<HTMLDivElement>(null);
    const contentRef = useRef<HTMLDivElement>(null);
    const [expandedKeys, setExpandedKeys] = useState<ReadonlySet<string>>(() => new Set());
    const [isFollowingLatest, setIsFollowingLatest] = useState(true);
    // Furthest offset reached without the user pulling back, so appended rows - which grow the
    // list without ever moving scrollTop - cannot be mistaken for a scroll away from the tail.
    const highWaterOffsetRef = useRef(0);

    // eslint-disable-next-line react-hooks/incompatible-library -- handled by "use no memo" above
    const virtualizer = useVirtualizer({
        count: batches.length,
        estimateSize: () => COLLAPSED_ROW_HEIGHT_IN_PX,
        getScrollElement: () => scrollRef.current,
        getItemKey: (index) => batches[index]?.key ?? index,
        overscan: OVERSCAN,
    });

    // The feed keeps appending, so pin the viewport to the newest batch while following is on.
    // Rows report their real height only once they have rendered, so the list still grows a
    // little after an append lands - watching the sizer re-pins on that late growth too, which
    // a `batches.length` effect alone would miss and leave the newest row half cut off.
    useLayoutEffect(() => {
        const scrollElement = scrollRef.current;
        const contentElement = contentRef.current;
        if (!isFollowingLatest || !scrollElement || !contentElement) {
            return;
        }

        const pinToBottom = () => {
            scrollElement.scrollTop = scrollElement.scrollHeight;
            highWaterOffsetRef.current = scrollElement.scrollTop;
        };

        pinToBottom();

        const observer = new ResizeObserver(pinToBottom);
        observer.observe(contentElement);

        return () => observer.disconnect();
    }, [isFollowingLatest]);

    // Scrolling up parks the view where the user left it, scrolling back to the bottom resumes.
    const handleScroll = (event: UIEvent<HTMLDivElement>) => {
        const { scrollTop, scrollHeight, clientHeight } = event.currentTarget;

        if (scrollTop < highWaterOffsetRef.current - SCROLL_UP_SLACK_IN_PX) {
            setIsFollowingLatest(false);
        } else if (scrollHeight - scrollTop - clientHeight <= AT_LATEST_THRESHOLD_IN_PX) {
            setIsFollowingLatest(true);
        }

        // Only downward travel moves the mark, so a slow drag upwards still adds up to a pause.
        highWaterOffsetRef.current = Math.max(highWaterOffsetRef.current, scrollTop);
    };

    const toggleExpanded = (key: string) => {
        setExpandedKeys((previous) => {
            const next = new Set(previous);
            if (!next.delete(key)) {
                next.add(key);
            }

            return next;
        });
    };

    return (
        <div className="overflow-hidden rounded-lg border">
            <div className="flex items-center justify-between gap-3 border-b bg-muted/40 px-3 py-2">
                <p className="text-xs text-muted-foreground tabular-nums">{formatCompact(batches.length)} batches</p>
                <div className="flex items-center gap-2">
                    <Label htmlFor={followId} className="text-xs font-normal text-muted-foreground">
                        Follow latest
                    </Label>
                    <Switch id={followId} checked={isFollowingLatest} onCheckedChange={setIsFollowingLatest} />
                </div>
            </div>
            <div
                ref={scrollRef}
                onScroll={handleScroll}
                className="overflow-y-auto overscroll-contain"
                style={{ height: "clamp(10rem, 40dvh, 26rem)" }}
            >
                <div ref={contentRef} className="relative w-full" style={{ height: virtualizer.getTotalSize() }}>
                    {virtualizer.getVirtualItems().map((virtualRow) => {
                        const batch = batches[virtualRow.index];

                        if (!batch) {
                            return null;
                        }

                        return (
                            <div
                                key={batch.key}
                                data-index={virtualRow.index}
                                ref={(node) => virtualizer.measureElement(node)}
                                // Positioned via top instead of translateY: Chromium never shrinks
                                // scrollable overflow contributed by transformed children, so rows
                                // that transiently render lower leave a permanent phantom scrollbar.
                                className="absolute inset-x-0"
                                style={{ top: virtualRow.start }}
                            >
                                <CdcBatchRow
                                    batch={batch}
                                    isExpanded={expandedKeys.has(batch.key)}
                                    // A `last:` selector would follow the rendered window, not the feed.
                                    isLast={virtualRow.index === batches.length - 1}
                                    onToggle={() => toggleExpanded(batch.key)}
                                />
                            </div>
                        );
                    })}
                </div>
            </div>
        </div>
    );
}

const BATCH_STATES: Record<CdcLiveBatchState, { label: string; tone: StatusTone; accentClassName: string }> = {
    success: {
        label: "Success",
        tone: "positive",
        accentClassName: "border-l-success",
    },
    pending: {
        label: "Pending",
        tone: "loading",
        accentClassName: "border-l-muted-foreground",
    },
    error: {
        label: "Error",
        tone: "danger",
        accentClassName: "border-l-destructive",
    },
};

function CdcBatchRow({
    batch,
    isExpanded,
    isLast,
    onToggle,
}: {
    batch: CdcLiveBatch;
    isExpanded: boolean;
    isLast: boolean;
    onToggle: () => void;
}) {
    const state = BATCH_STATES[batch.state];
    const errorCount = batch.scriptErrors + batch.readErrors;

    return (
        <div className={cn("border-l-2", !isLast && "border-b", state.accentClassName, isExpanded && "bg-muted/30")}>
            <button
                type="button"
                onClick={onToggle}
                aria-expanded={isExpanded}
                className="flex h-10 w-full items-center gap-2 px-3 text-left transition-colors hover:bg-muted/50 focus-visible:ring-[3px] focus-visible:ring-ring/50 focus-visible:outline-none sm:gap-3"
            >
                <ChevronRight
                    aria-hidden={true}
                    className={cn(
                        "size-3.5 shrink-0 text-muted-foreground transition-transform",
                        isExpanded && "rotate-90",
                    )}
                />
                <StatusIndicator tone={state.tone} label={state.label} className="shrink-0 justify-start" />
                <span className="shrink-0 font-mono text-xs text-muted-foreground tabular-nums">
                    {formatBatchTime(batch.started)}
                </span>
                <span className="min-w-0 flex-1 truncate text-xs text-muted-foreground">
                    {describeBatch(batch, errorCount)}
                </span>
                {/* The narrow layout keeps state, time and summary; the duration stays in the details below. */}
                <span className="hidden shrink-0 font-mono text-xs text-muted-foreground tabular-nums sm:inline">
                    {formatBatchDuration(batch.durationInMs)}
                </span>
            </button>
            {isExpanded && (
                <dl className="grid grid-cols-[auto_1fr] gap-x-6 gap-y-1.5 px-3 pb-3 pl-10">
                    <CdcBatchDetail label="Started" value={formatBatchDateTime(batch.started)} />
                    <CdcBatchDetail
                        label="Completed"
                        value={batch.ended ? formatBatchDateTime(batch.ended) : "Still in progress"}
                    />
                    <CdcBatchDetail label="Duration" value={formatBatchDuration(batch.durationInMs)} />
                    <CdcBatchDetail label="Messages processed" value={batch.processed.toLocaleString()} />
                    {batch.scriptErrors > 0 && (
                        <CdcBatchDetail
                            label="Script errors"
                            value={batch.scriptErrors.toLocaleString()}
                            isDestructive
                        />
                    )}
                    {batch.readErrors > 0 && (
                        <CdcBatchDetail label="Read errors" value={batch.readErrors.toLocaleString()} isDestructive />
                    )}
                </dl>
            )}
        </div>
    );
}

function CdcBatchDetail({
    label,
    value,
    isDestructive = false,
}: {
    label: string;
    value: ReactNode;
    isDestructive?: boolean;
}) {
    return (
        <>
            <dt className="text-xs text-muted-foreground">{label}</dt>
            <dd
                className={cn(
                    "font-mono text-xs break-words tabular-nums",
                    isDestructive ? "text-destructive" : "text-foreground",
                )}
            >
                {value}
            </dd>
        </>
    );
}

function describeBatch(batch: CdcLiveBatch, errorCount: number): string {
    const messages = `${formatCompact(batch.processed)} ${batch.processed === 1 ? "message" : "messages"}`;

    if (batch.state === "error") {
        return `${formatCompact(errorCount)} ${errorCount === 1 ? "error" : "errors"}, ${messages} processed`;
    }

    return batch.state === "pending" ? `Processing ${messages}` : `Processed ${messages}`;
}

const TIME_WITH_MS_OPTIONS: Intl.DateTimeFormatOptions = {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    fractionalSecondDigits: 3,
    hour12: false,
};

const batchTimeFormatter = new Intl.DateTimeFormat("en-GB", TIME_WITH_MS_OPTIONS);
const batchDateTimeFormatter = new Intl.DateTimeFormat("en-GB", {
    day: "2-digit",
    month: "short",
    year: "numeric",
    ...TIME_WITH_MS_OPTIONS,
});

function formatBatchTime(value: string): string {
    return format(batchTimeFormatter, value);
}

function formatBatchDateTime(value: string): string {
    return format(batchDateTimeFormatter, value);
}

function format(formatter: Intl.DateTimeFormat, value: string): string {
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? value : formatter.format(date);
}

const SECOND_IN_MS = 1000;

function formatBatchDuration(durationInMs: number): string {
    return durationInMs < SECOND_IN_MS
        ? `${Math.round(durationInMs)} ms`
        : `${(durationInMs / SECOND_IN_MS).toFixed(2)} s`;
}
