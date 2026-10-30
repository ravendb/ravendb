import { ChevronsDownUp, ChevronsUpDown, RotateCcw, Search, X } from "lucide-react";
import { Button } from "@/components/shadcn/ui/button";
import { Input } from "@/components/shadcn/ui/input";
import { Skeleton } from "@/components/shadcn/ui/skeleton";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/shadcn/ui/tooltip";
import { Heading, Text } from "@/components/typography";
import { formatCount } from "@/lib/format";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { PlanAreaCard } from "@/pages/setup/add-app-wizard/steps/map/plan/plan-area-card";
import { PlanConventionsMenu } from "@/pages/setup/add-app-wizard/steps/map/plan/plan-conventions-menu";
import { PlanLeftOut } from "@/pages/setup/add-app-wizard/steps/map/plan/plan-left-out";
import { PlanStatusLine } from "@/pages/setup/add-app-wizard/steps/map/plan/plan-status-line";
import { createNameMatcher, type Plan } from "@/pages/setup/add-app-wizard/steps/map/session/planner-plan";
import { isChatWorking } from "@/pages/setup/add-app-wizard/steps/map/session/planner-state";

export function PlanView({ plan }: { plan: Plan }) {
    const filter = useSetupWizardStore((state) => state.plannerFilter);
    const hasProposal = useSetupWizardStore((state) => state.plannerProposal !== null);
    const isProposing = useSetupWizardStore((state) => isChatWorking(state.plannerProposalChat));
    const tableCount = useSetupWizardStore((state) => state.plannerSelectedTables?.length ?? 0);
    const startPlannerOver = useSetupWizardStore((state) => state.startPlannerOver);
    const matches = createNameMatcher(filter);
    const isFiltering = filter.trim() !== "";
    const areas = plan.areas
        .map((area) => ({
            ...area,
            collections: area.collections.filter((collection) => matches(collection.searchKey)),
        }))
        .filter((area) => !isFiltering || area.collections.length > 0);
    const leftOut = plan.leftOut.filter((table) => matches(table.searchKey));
    const collectionCount = plan.collections.filter((collection) => collection.state !== "removed").length;

    return (
        <div className="mx-auto flex w-full max-w-280 flex-col gap-4 px-4 py-5 2xl:px-7">
            <div className="flex flex-col gap-3">
                <div className="flex flex-wrap items-center gap-x-4 gap-y-3">
                    <Heading as="h2" variant="section" className="mr-auto">
                        {hasProposal
                            ? `${formatCount(collectionCount, "collection")} from ${formatCount(tableCount, "table")}`
                            : "The plan"}
                    </Heading>
                    <PlanConventionsMenu />
                </div>
                {hasProposal && (
                    <div className="flex flex-wrap items-center gap-3">
                        <PlanFilter />
                        <ExpandAllAreasButton plan={plan} />
                    </div>
                )}
            </div>

            <PlanStatusLine plan={plan} />

            {!hasProposal && isProposing && <AreaSkeletons />}

            {areas.length > 0 && (
                <section aria-labelledby="plan-areas-title" className="flex flex-col gap-1.5">
                    <Heading as="h3" variant="label" id="plan-areas-title" className="px-0.5">
                        Areas{" "}
                        <Text as="span" variant="caption" className="font-normal">
                            {areas.length}
                        </Text>
                    </Heading>
                    <div className="flex flex-col gap-2">
                        {areas.map((area) => (
                            <PlanAreaCard key={area.name} area={area} isFiltering={isFiltering} />
                        ))}
                    </div>
                </section>
            )}

            {hasProposal && isFiltering && areas.length === 0 && leftOut.length === 0 && (
                <Text className="text-text-soft">No collection or table matches &ldquo;{filter.trim()}&rdquo;.</Text>
            )}

            {hasProposal && <PlanLeftOut tables={leftOut} isFiltering={isFiltering} />}

            {!hasProposal && !isProposing && (
                <div className="flex flex-col items-start gap-2.5">
                    <Text className="text-text-soft">The planner stopped before it proposed a plan.</Text>
                    <Button variant="outline" size="sm" onClick={startPlannerOver}>
                        <RotateCcw data-icon="inline-start" />
                        Start over
                    </Button>
                </div>
            )}
        </div>
    );
}

function PlanFilter() {
    const filter = useSetupWizardStore((state) => state.plannerFilter);
    const setPlannerFilter = useSetupWizardStore((state) => state.setPlannerFilter);

    return (
        <div className="relative flex min-w-50 flex-[0_1_17.5rem] items-center">
            <Search className="pointer-events-none absolute left-2.5 size-4 text-muted-foreground" aria-hidden="true" />
            <Input
                type="search"
                aria-label="Filter the plan by collection or table name"
                placeholder="Filter by collection or table"
                value={filter}
                className="pr-8 pl-8"
                onChange={(event) => setPlannerFilter(event.target.value)}
                onKeyDown={(event) => {
                    if (event.key === "Escape" && filter) {
                        event.preventDefault();
                        setPlannerFilter("");
                    }
                }}
            />
            {filter && (
                <Button
                    variant="ghost"
                    size="icon-sm"
                    className="absolute right-0.5 text-muted-foreground"
                    aria-label="Clear the filter"
                    onClick={() => setPlannerFilter("")}
                >
                    <X aria-hidden="true" />
                </Button>
            )}
        </div>
    );
}

/** Only areas that have not started fold open on the plan; the others open on their own page. */
function ExpandAllAreasButton({ plan }: { plan: Plan }) {
    const expandedAreas = useSetupWizardStore((state) => state.plannerExpandedAreas);
    const setPlannerExpandedAreas = useSetupWizardStore((state) => state.setPlannerExpandedAreas);
    const expandable = plan.areas.filter((area) => !area.isStarted).map((area) => area.name);
    const isAllOpen = expandable.length > 0 && expandable.every((area) => expandedAreas.includes(area));
    const label = isAllOpen ? "Collapse all areas" : "Expand all areas";

    if (expandable.length === 0) {
        return null;
    }

    return (
        <Tooltip>
            <TooltipTrigger asChild>
                <Button
                    variant="outline"
                    size="icon"
                    aria-label={label}
                    onClick={() => setPlannerExpandedAreas(isAllOpen ? [] : expandable)}
                >
                    {isAllOpen ? <ChevronsDownUp aria-hidden="true" /> : <ChevronsUpDown aria-hidden="true" />}
                </Button>
            </TooltipTrigger>
            <TooltipContent>{label}</TooltipContent>
        </Tooltip>
    );
}

function AreaSkeletons() {
    return (
        <div aria-hidden="true" className="flex flex-col gap-2">
            {[0, 1, 2].map((index) => (
                <div key={index} className="flex gap-3 rounded-lg border bg-card px-4 py-3.5">
                    <Skeleton className="mt-0.75 size-4 shrink-0 rounded-full" />
                    <div className="flex flex-1 flex-col gap-2.5">
                        <div className="flex items-center gap-2.5">
                            <Skeleton className="h-3.5 w-30" />
                            <Skeleton className="h-3.5 w-16 rounded-full" />
                        </div>
                        <Skeleton className="h-2.5 w-[92%]" />
                        <Skeleton className="h-2.5 w-[60%]" />
                    </div>
                </div>
            ))}
        </div>
    );
}
