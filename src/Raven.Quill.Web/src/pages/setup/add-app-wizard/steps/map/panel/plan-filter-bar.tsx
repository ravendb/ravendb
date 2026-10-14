import { Search, X } from "lucide-react";
import { SELECTED_CARD_CLASSES } from "@/components/form/form-radio-cards";
import { Button } from "@/components/shadcn/ui/button";
import { InputGroup, InputGroupAddon, InputGroupButton, InputGroupInput } from "@/components/shadcn/ui/input-group";
import { ToggleGroup, ToggleGroupItem } from "@/components/shadcn/ui/toggle-group";
import { Text } from "@/components/typography";
import { formatCount } from "@/lib/format";
import { cn } from "@/lib/utils";
import { ALL_STATES, type PlanFilters } from "@/pages/setup/add-app-wizard/steps/map/panel/use-plan-filters";
import {
    countByGroup,
    countByState,
    PLAN_STATE_LABELS,
    PLAN_TABLE_GROUP_LABELS,
    type PlanCollection,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-plan";

type StateChip<Key extends string> = {
    key: Key | typeof ALL_STATES;
    label: string;
    count: number | null;
    isDanger: boolean;
};

export function PlanFilterBar({ filters, collections }: { filters: PlanFilters; collections: PlanCollection[] }) {
    const isTablesTab = filters.tab === "tables";
    const shownCount = isTablesTab ? filters.shownTables.length : filters.shownCollections.length;

    return (
        <div className="mr-2 flex flex-col gap-2">
            <InputGroup className="bg-background dark:bg-background">
                <InputGroupAddon>
                    <Search aria-hidden="true" />
                </InputGroupAddon>
                <InputGroupInput
                    type="search"
                    aria-label={
                        isTablesTab
                            ? "Filter the tables by table or collection name"
                            : "Filter the plan by collection or table name"
                    }
                    placeholder={
                        isTablesTab ? "Filter by table or collection name" : "Filter by collection or table name"
                    }
                    value={filters.filter}
                    className="[&::-webkit-search-cancel-button]:appearance-none"
                    onChange={(event) => filters.setFilter(event.target.value)}
                    onKeyDown={(event) => {
                        if (event.key === "Escape" && filters.filter) {
                            event.preventDefault();
                            event.stopPropagation();
                            filters.setFilter("");
                        }
                    }}
                />
                {filters.filter && (
                    <InputGroupAddon align="inline-end">
                        <InputGroupButton
                            size="icon-xs"
                            aria-label="Clear the name filter"
                            onClick={() => filters.setFilter("")}
                        >
                            <X aria-hidden="true" />
                        </InputGroupButton>
                    </InputGroupAddon>
                )}
            </InputGroup>

            {!isTablesTab && filters.focusedCollection && (
                <div className="flex flex-wrap items-center gap-x-2">
                    <Text variant="muted" as="span">
                        Showing{" "}
                        <Text variant="label" as="span">
                            {filters.focusedCollection}
                        </Text>{" "}
                        only
                    </Text>
                    <Button variant="link" className="h-auto p-0" onClick={filters.clearFilters}>
                        Show all
                    </Button>
                </div>
            )}

            {isTablesTab ? (
                <StateChips
                    chips={getChips(
                        PLAN_TABLE_GROUP_LABELS,
                        countByGroup(filters.tables),
                        countByGroup(filters.matchingTables),
                        filters.tableGroup,
                    )}
                    value={filters.tableGroup}
                    onValueChange={filters.setTableGroup}
                />
            ) : (
                <StateChips
                    chips={getChips(
                        PLAN_STATE_LABELS,
                        countByState(collections),
                        countByState(filters.matchingCollections),
                        filters.collectionState,
                    )}
                    value={filters.collectionState}
                    onValueChange={filters.setCollectionState}
                />
            )}

            <span role="status" className="sr-only">
                {filters.isFiltering &&
                    (shownCount > 0
                        ? `${formatCount(shownCount, isTablesTab ? "table" : "collection")} shown`
                        : "Nothing matches")}
            </span>
        </div>
    );
}

function StateChips<Key extends string>({
    chips,
    value,
    onValueChange,
}: {
    chips: StateChip<Key>[];
    value: Key | typeof ALL_STATES;
    onValueChange: (value: Key | typeof ALL_STATES) => void;
}) {
    return (
        <ToggleGroup
            type="single"
            variant="outline"
            spacing={1.5}
            aria-label="Filter by state"
            className="flex-wrap"
            value={value}
            onValueChange={(next) => next && onValueChange(next as Key | typeof ALL_STATES)}
        >
            {chips.map((chip) => (
                <ToggleGroupItem
                    key={chip.key}
                    value={chip.key}
                    className={cn(
                        "h-7 gap-1.5 rounded-full border-input bg-transparent px-2.5",
                        chip.key === value && SELECTED_CARD_CLASSES,
                    )}
                >
                    {chip.label}
                    {chip.count !== null && (
                        <span
                            className={cn(
                                "text-muted-foreground tabular-nums",
                                chip.isDanger && "text-badge-destructive-fg",
                            )}
                        >
                            {chip.count}
                        </span>
                    )}
                </ToggleGroupItem>
            ))}
        </ToggleGroup>
    );
}

/** A chip per state the plan has, or the one filtered on; each counts what matches the name filter. */
function getChips<Key extends string>(
    labels: Record<Key, string>,
    allCounts: Record<Key, number>,
    matchingCounts: Record<Key, number>,
    active: Key | typeof ALL_STATES,
): StateChip<Key>[] {
    const keys = Object.keys(labels) as Key[];

    return [
        { key: ALL_STATES, label: "All", count: null, isDanger: false },
        ...keys
            .filter((key) => allCounts[key] > 0 || key === active)
            .map((key) => ({
                key,
                label: labels[key],
                count: matchingCounts[key],
                isDanger: key === "failed" && matchingCounts[key] > 0,
            })),
    ];
}
