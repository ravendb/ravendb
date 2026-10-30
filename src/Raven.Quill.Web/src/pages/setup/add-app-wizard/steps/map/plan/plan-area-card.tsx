import { ArrowRight, ChevronRight } from "lucide-react";
import { Button } from "@/components/shadcn/ui/button";
import { HoverCard, HoverCardContent, HoverCardTrigger } from "@/components/shadcn/ui/hover-card";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { Heading, Text } from "@/components/typography";
import { cn } from "@/lib/utils";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { BuildButton } from "@/pages/setup/add-app-wizard/steps/map/plan/build-button";
import {
    PlanCollectionSources,
    PlanProblems,
    PlanStatusBadge,
} from "@/pages/setup/add-app-wizard/steps/map/plan/plan-collection-parts";
import {
    countBuilt,
    getAreaShortName,
    isMapped,
    type PlanArea,
    type PlanCollection,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-plan";
import { useBuildBlockReason, usePlannerActions } from "@/pages/setup/add-app-wizard/steps/map/use-planner-actions";

type SquareKind = "built" | "warning" | "failed" | "building" | "pending" | "removed";

const SQUARE_CLASSES: Record<SquareKind, string> = {
    built: "bg-success",
    warning: "bg-warning",
    failed: "bg-destructive",
    building: "animate-pulse bg-primary motion-reduce:animate-none",
    pending: "bg-muted-foreground/30",
    removed: "bg-muted-foreground/30 opacity-45",
};

const SQUARE_LABELS: Record<SquareKind, (count: number) => string> = {
    built: (count) => `${count} built`,
    warning: (count) => `${count} built with ${count === 1 ? "a warning" : "warnings"}`,
    failed: (count) => `${count} failed`,
    building: (count) => `${count} building`,
    pending: (count) => `${count} not built yet`,
    removed: (count) => `${count} removed`,
};

/**
 * One area of the plan. Until it starts building, its header folds its collections open; once it has
 * started, the header opens the area itself, where its agent works.
 */
export function PlanAreaCard({ area, isFiltering }: { area: PlanArea; isFiltering: boolean }) {
    const actions = usePlannerActions();
    const buildBlockReason = useBuildBlockReason();
    const isExpanded = useSetupWizardStore((state) => state.plannerExpandedAreas.includes(area.name));
    const togglePlannerArea = useSetupWizardStore((state) => state.togglePlannerArea);
    const collections = area.collections.filter((collection) => collection.state !== "removed");
    const builtCount = countBuilt(collections);
    const failedCount = collections.filter((collection) => collection.state === "failed").length;
    const isOpen = !area.isStarted && (isExpanded || isFiltering);
    const canBuild = area.pendingCollections.length > 0 && !area.isBuilding;
    const rowsId = `plan-area-${area.name}`;

    return (
        <section className="flex flex-col rounded-lg border bg-card px-4 py-3" aria-label={area.name}>
            <div className="flex flex-wrap items-center gap-x-2.5 gap-y-1.5">
                <Heading as="h4" variant="subsection" className="min-w-0 flex-1">
                    <button
                        type="button"
                        className="flex w-full items-center gap-2.5 text-left"
                        aria-expanded={area.isStarted ? undefined : isOpen}
                        aria-controls={area.isStarted ? undefined : rowsId}
                        onClick={() => (area.isStarted ? actions.openArea(area.name) : togglePlannerArea(area.name))}
                    >
                        {!area.isStarted && (
                            <ChevronRight
                                className={cn(
                                    "size-3.5 shrink-0 text-muted-foreground transition-transform motion-reduce:transition-none",
                                    isOpen && "rotate-90",
                                )}
                                aria-hidden="true"
                            />
                        )}
                        <span className="truncate">{getAreaShortName(area.name)}</span>
                        <AreaStrip area={area} collections={collections} />
                        <Text as="span" variant="caption" className="flex shrink-0 items-center gap-1.5 tabular-nums">
                            {area.isBuilding && <Spinner className="size-3 text-primary-strong" aria-hidden="true" />}
                            {builtCount}/{collections.length}
                            {failedCount > 0 && (
                                <span className="font-medium text-badge-destructive-fg">{failedCount} failed</span>
                            )}
                        </Text>
                    </button>
                </Heading>
                {canBuild ? (
                    <BuildButton
                        label={area.isStarted ? "Build the rest" : "Build"}
                        blockReason={buildBlockReason}
                        onBuild={() => actions.buildAndOpenArea(area)}
                    />
                ) : (
                    <Button variant="outline" size="sm" onClick={() => actions.openArea(area.name)}>
                        Open
                        <ArrowRight data-icon="inline-end" />
                    </Button>
                )}
            </div>
            {isOpen && (
                <ul id={rowsId} className="flex flex-col gap-2 pt-2.5">
                    {area.collections.map((collection) => (
                        <PlanAreaRow key={collection.name} collection={collection} />
                    ))}
                </ul>
            )}
        </section>
    );
}

function AreaStrip({ area, collections }: { area: PlanArea; collections: PlanCollection[] }) {
    const kinds = area.collections.map(getSquareKind);
    const counts = new Map<SquareKind, number>();

    for (const kind of kinds) {
        counts.set(kind, (counts.get(kind) ?? 0) + 1);
    }

    return (
        <HoverCard openDelay={300} closeDelay={0}>
            <HoverCardTrigger asChild>
                <span className="flex max-w-55 flex-wrap items-center gap-0.75 py-1.5" aria-hidden="true">
                    {kinds.map((kind, index) => (
                        <span key={index} className={cn("size-2.5 shrink-0 rounded-[2px]", SQUARE_CLASSES[kind])} />
                    ))}
                </span>
            </HoverCardTrigger>
            <HoverCardContent side="bottom" align="start" className="flex w-auto flex-col gap-1.5 px-3 py-2.5">
                <Text as="span" variant="label">
                    {area.name}, {countBuilt(collections)} of {collections.length} built
                </Text>
                {[...counts].map(([kind, count]) => (
                    <Text key={kind} as="span" className="flex items-center gap-2">
                        <span className={cn("size-2.5 shrink-0 rounded-[2px]", SQUARE_CLASSES[kind])} />
                        {SQUARE_LABELS[kind](count)}
                    </Text>
                ))}
                <Text as="span" variant="caption">
                    Each square is one collection of this area.
                </Text>
            </HoverCardContent>
        </HoverCard>
    );
}

function getSquareKind(collection: PlanCollection): SquareKind {
    switch (collection.state) {
        case "removed":
            return "removed";
        case "failed":
            return "failed";
        case "building":
            return "building";
        case "suggested":
            return "pending";
        default:
            return collection.warnings.length > 0 ? "warning" : "built";
    }
}

function PlanAreaRow({ collection }: { collection: PlanCollection }) {
    const actions = usePlannerActions();
    const content = (
        <>
            <span className="flex flex-wrap items-center gap-x-2 gap-y-1">
                <Text as="span" variant="label">
                    {collection.name}
                </Text>
                <PlanStatusBadge collection={collection} />
            </span>
            {collection.state !== "failed" && collection.why && (
                <Text className="line-clamp-2 text-text-soft">{collection.why}</Text>
            )}
            <PlanCollectionSources collection={collection} />
        </>
    );

    return (
        <li
            className={cn(
                "flex flex-col gap-1.5 rounded-md border bg-surface2 px-3 py-2.5",
                collection.state === "failed" && "border-badge-destructive-border",
                collection.state === "removed" && "border-dashed bg-transparent",
            )}
        >
            {isMapped(collection) ? (
                <button
                    type="button"
                    className="flex flex-col gap-1.5 text-left"
                    onClick={() => actions.openCollection(collection.area, collection.name)}
                >
                    {content}
                </button>
            ) : (
                <div className="flex flex-col gap-1.5">{content}</div>
            )}
            {collection.state === "failed" && <PlanProblems problems={collection.problems} />}
        </li>
    );
}
