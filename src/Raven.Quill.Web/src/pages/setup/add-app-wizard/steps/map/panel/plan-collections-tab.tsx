import { useState } from "react";
import { CountBadge } from "@/components/data/count-badge";
import { Button } from "@/components/shadcn/ui/button";
import { Collapsible, CollapsibleContent } from "@/components/shadcn/ui/collapsible";
import { Heading, Text } from "@/components/typography";
import { cn } from "@/lib/utils";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { CollapsibleHeader } from "@/pages/setup/add-app-wizard/steps/map/chat/planner-activity-parts";
import { PlanCollectionCard } from "@/pages/setup/add-app-wizard/steps/map/panel/plan-collection-card";
import { PlanFilterEmpty } from "@/pages/setup/add-app-wizard/steps/map/panel/plan-filter-empty";
import { ALL_STATES, type PlanFilters } from "@/pages/setup/add-app-wizard/steps/map/panel/use-plan-filters";
import { PlannerVirtualList } from "@/pages/setup/add-app-wizard/steps/map/planner-virtual-list";
import {
    groupByArea,
    PLAN_STATE_LABELS,
    type PlanCollection,
    type PlanCollectionState,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-plan";
import { selectBuiltCollections } from "@/pages/setup/add-app-wizard/steps/map/session/planner-state";
import type { PlannerProposal } from "@/pages/setup/add-app-wizard/steps/map/session/planner-types";

type RowGap = "none" | "item" | "group";

type CollectionRow =
    | { kind: "area"; key: string; name: string; gap: RowGap }
    | { kind: "collection"; key: string; collection: PlanCollection; gap: RowGap; isInArea: boolean };

const GAP_CLASSES: Record<RowGap, string> = { none: "", item: "pt-2", group: "pt-5" };

const AREA_ROW_HEIGHT_PX = 28;

const COLLECTION_ROW_HEIGHT_PX = 110;

export function PlanCollectionsTab({ filters }: { filters: PlanFilters }) {
    const proposal = useSetupWizardStore((state) => state.plannerProposal);
    const hasBuiltAny = useSetupWizardStore((state) => selectBuiltCollections(state).length > 0);
    const enables = proposal?.enables ?? [];
    const tablesWithoutCollection = filters.matchingTables.filter((table) => table.group !== "inCollections").length;
    const rows = toRows(filters.shownCollections, proposal);

    return (
        <PlannerVirtualList
            items={rows}
            getKey={(row) => row.key}
            estimateSize={(row) => (row.kind === "area" ? AREA_ROW_HEIGHT_PX : COLLECTION_ROW_HEIGHT_PX)}
            className="p-4"
            renderItem={(row) => (
                <div className={GAP_CLASSES[row.gap]}>
                    {row.kind === "area" ? (
                        <Heading as="h3" variant="label" className="text-muted-foreground">
                            {row.name}
                        </Heading>
                    ) : (
                        <PlanCollectionCard collection={row.collection} headingAs={row.isInArea ? "h4" : "h3"} />
                    )}
                </div>
            )}
        >
            <div className={cn("flex flex-col gap-5 empty:hidden", rows.length > 0 && "pt-5")}>
                {filters.isFiltering && filters.shownCollections.length === 0 && (
                    <PlanFilterEmpty
                        message={describeNoCollections(filters.filter, filters.collectionState)}
                        onClearFilters={filters.clearFilters}
                    />
                )}

                {filters.collectionState === ALL_STATES && tablesWithoutCollection > 0 && (
                    <Text variant="muted">
                        {describeTablesWithoutCollection(tablesWithoutCollection, filters.filter.trim() !== "")}{" "}
                        <Button variant="link" className="h-auto p-0" onClick={filters.showTables}>
                            Show in Tables
                        </Button>
                    </Text>
                )}

                {enables.length > 0 && !hasBuiltAny && !filters.isFiltering && <PlanEnables enables={enables} />}
            </div>
        </PlannerVirtualList>
    );
}

function toRows(collections: PlanCollection[], proposal: PlannerProposal | null): CollectionRow[] {
    return groupByArea(collections, proposal).flatMap((area, areaIndex) => {
        const areaGap: RowGap = areaIndex === 0 ? "none" : "group";
        const header: CollectionRow[] = area.name
            ? [{ kind: "area", key: `area:${area.name}`, name: area.name, gap: areaGap }]
            : [];

        return [
            ...header,
            ...area.collections.map(
                (collection, index): CollectionRow => ({
                    kind: "collection",
                    key: `collection:${collection.name}`,
                    collection,
                    gap: index > 0 || area.name ? "item" : areaGap,
                    isInArea: area.name !== null,
                }),
            ),
        ];
    });
}

/** What the proposed document model makes possible: the planner's case for the plan. */
function PlanEnables({ enables }: { enables: string[] }) {
    const [isOpen, setIsOpen] = useState(false);

    return (
        <Collapsible open={isOpen} onOpenChange={setIsOpen} className="rounded-lg border">
            <CollapsibleHeader isOpen={isOpen} className="px-2.5">
                <Text variant="label" as="span">
                    What this enables
                </Text>
                <CountBadge className="ml-auto">{enables.length}</CountBadge>
            </CollapsibleHeader>
            <CollapsibleContent>
                <ul className="flex list-disc flex-col gap-1 pr-3 pb-2.5 pl-10">
                    {enables.map((enabled, index) => (
                        <li key={index}>
                            <Text as="span" className="text-text-soft">
                                {enabled}
                            </Text>
                        </li>
                    ))}
                </ul>
            </CollapsibleContent>
        </Collapsible>
    );
}

function describeNoCollections(filter: string, state: PlanCollectionState | typeof ALL_STATES): string {
    const typed = filter.trim();
    const label = state === ALL_STATES ? "" : `${PLAN_STATE_LABELS[state].toLowerCase()} `;

    return typed ? `No ${label}collection matches "${typed}".` : `No ${label}collections right now.`;
}

function describeTablesWithoutCollection(count: number, isFiltered: boolean): string {
    const table = isFiltered ? "matching table" : "table";

    return count === 1 ? `1 ${table} isn't in a collection.` : `${count} ${table}s aren't in a collection.`;
}
