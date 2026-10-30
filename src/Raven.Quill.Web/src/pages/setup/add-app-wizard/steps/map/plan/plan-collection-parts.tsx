import type { ReactNode } from "react";
import { CircleX, CornerDownRight, Link, Table, TriangleAlert, type LucideIcon } from "lucide-react";
import { StatusIndicator, type StatusTone } from "@/components/data/status-indicator";
import { Alert, AlertDescription } from "@/components/shadcn/ui/alert";
import { Badge } from "@/components/shadcn/ui/badge";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/shadcn/ui/tooltip";
import { Text } from "@/components/typography";
import { joinList } from "@/lib/format";
import { cn } from "@/lib/utils";
import {
    getCollectionSources,
    PLAN_STATE_LABELS,
    type CollectionSource,
    type CollectionSourceKind,
    type PlanCollection,
    type PlanCollectionState,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-plan";

const STATE_TONES: Record<PlanCollectionState, StatusTone> = {
    suggested: "muted",
    building: "loading",
    built: "positive",
    updated: "info",
    failed: "danger",
    removed: "muted",
};

const SOURCE_KINDS: Record<CollectionSourceKind, { icon: LucideIcon; label: string }> = {
    table: { icon: Table, label: "Table" },
    embed: { icon: CornerDownRight, label: "Embedded" },
    link: { icon: Link, label: "Links to" },
};

/**
 * A collection not built yet shows no badge: every collection of the plan starts that way. A failed one
 * already lists its problems in red beside the badge, so the badge itself stays plain.
 */
export function PlanStatusBadge({ collection, className }: { collection: PlanCollection; className?: string }) {
    if (collection.state === "suggested") {
        return null;
    }

    if (collection.state === "failed" && collection.retryErrors.length === 0) {
        return (
            <Badge variant="destructive" className={className}>
                {PLAN_STATE_LABELS.failed}
            </Badge>
        );
    }

    return (
        <StatusIndicator
            tone={STATE_TONES[collection.state]}
            label={collection.retryErrors.length > 0 ? "Retrying" : PLAN_STATE_LABELS[collection.state]}
            className={className}
        />
    );
}

export function PlanProblems({ problems, className }: { problems: string[]; className?: string }) {
    if (problems.length === 0) {
        return null;
    }

    return (
        <ul className={cn("flex flex-col gap-1.5 rounded-md bg-badge-destructive-bg px-2.5 py-2", className)}>
            {problems.map((problem, index) => (
                <li key={index} className="flex items-start gap-2">
                    <CircleX className="mt-0.5 size-3.5 shrink-0 text-badge-destructive-fg" aria-hidden="true" />
                    <Text as="span">{problem}</Text>
                </li>
            ))}
        </ul>
    );
}

export function PlanWarning({ className, children }: { className?: string; children: ReactNode }) {
    return (
        <Alert variant="warning" role="note" className={className}>
            <TriangleAlert aria-hidden="true" />
            <AlertDescription>{children}</AlertDescription>
        </Alert>
    );
}

/** One line on a card: each kind of source shows its first, and the rest sit behind a count. */
export function PlanCollectionSources({ collection, className }: { collection: PlanCollection; className?: string }) {
    const { table, embeds, links } = getCollectionSources(collection);

    if (!table) {
        return null;
    }

    return (
        <Text
            as="span"
            variant="caption"
            className={cn("grid auto-cols-[minmax(0,max-content)] grid-flow-col justify-start gap-x-4", className)}
        >
            <SourceSlot sources={[table]} title={`Built from the ${table.name} table`} />
            {embeds.length > 0 && (
                <SourceSlot
                    sources={embeds}
                    title={embeds
                        .map(
                            (embed) => `${embed.name} is embedded in each document${embed.as ? ` as ${embed.as}` : ""}`,
                        )
                        .join(". ")}
                />
            )}
            {links.length > 0 && (
                <SourceSlot
                    sources={links}
                    title={`Links to ${joinList(links.map((link) => link.name))} documents by their id`}
                />
            )}
        </Text>
    );
}

/** Every source on its own, where there is room for all of them. */
export function PlanSourceList({ sources }: { sources: CollectionSource[] }) {
    return (
        <Text as="span" variant="caption" className="flex flex-wrap gap-x-4 gap-y-1">
            {sources.map((source) => {
                const { icon: Icon, label } = SOURCE_KINDS[source.kind];

                return (
                    <span key={`${source.kind}-${source.name}-${source.as}`} className="flex items-center gap-1.25">
                        <Icon className="size-3.5 shrink-0" aria-hidden="true" />
                        <span className="sr-only">{label}: </span>
                        <SourceName source={source} className="text-text-soft" />
                    </span>
                );
            })}
        </Text>
    );
}

function SourceSlot({ sources, title }: { sources: CollectionSource[]; title: string }) {
    const [first, ...rest] = sources;
    const { icon: Icon, label } = SOURCE_KINDS[first.kind];

    return (
        <span className="flex min-w-0 items-center gap-1.25">
            <Icon className="size-3.5 shrink-0" aria-hidden="true" />
            <Tooltip>
                <TooltipTrigger asChild>
                    <span className="min-w-0 truncate">
                        <span className="sr-only">{label}: </span>
                        <SourceName source={first} className="text-text-soft" />
                        {rest.length > 0 && (
                            <span className="sr-only">
                                {rest.map((source) => `, ${source.name}${source.as ? ` as ${source.as}` : ""}`)}
                            </span>
                        )}
                    </span>
                </TooltipTrigger>
                <TooltipContent className="max-w-80">{title}</TooltipContent>
            </Tooltip>
            {rest.length > 0 && (
                <Tooltip>
                    <TooltipTrigger asChild>
                        <span aria-hidden="true" className="shrink-0 rounded-sm border bg-muted px-1.25 tabular-nums">
                            +{rest.length}
                        </span>
                    </TooltipTrigger>
                    <TooltipContent className="flex-col items-start gap-1">
                        <span className="font-semibold">{first.kind === "link" ? "Also links to" : "Also embeds"}</span>
                        {rest.map((source) => (
                            <span key={`${source.name}-${source.as}`}>
                                <SourceName source={source} />
                            </span>
                        ))}
                    </TooltipContent>
                </Tooltip>
            )}
        </span>
    );
}

function SourceName({ source, className }: { source: CollectionSource; className?: string }) {
    return (
        <>
            <span className={cn(source.kind !== "link" && "font-mono", className)}>{source.name}</span>
            {source.as && ` as ${source.as}`}
        </>
    );
}
