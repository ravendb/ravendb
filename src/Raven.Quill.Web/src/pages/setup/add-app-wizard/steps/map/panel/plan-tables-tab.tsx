import { useState } from "react";
import { ChevronRight, Sparkle } from "lucide-react";
import { Button } from "@/components/shadcn/ui/button";
import { Checkbox } from "@/components/shadcn/ui/checkbox";
import { Heading, Text } from "@/components/typography";
import { formatCount } from "@/lib/format";
import { cn } from "@/lib/utils";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { PlanFilterEmpty } from "@/pages/setup/add-app-wizard/steps/map/panel/plan-filter-empty";
import { PlanStatusBadge } from "@/pages/setup/add-app-wizard/steps/map/panel/plan-status-badge";
import { ALL_STATES, type PlanFilters } from "@/pages/setup/add-app-wizard/steps/map/panel/use-plan-filters";
import { PlannerVirtualList } from "@/pages/setup/add-app-wizard/steps/map/planner-virtual-list";
import {
    useCanComposeForPlanner,
    useFocusPlannerComposer,
} from "@/pages/setup/add-app-wizard/steps/map/planner-workspace-context";
import {
    PLAN_TABLE_GROUP_LABELS,
    type PlanTable,
    type PlanTableGroup,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-plan";
import { composeAddTablesPrompt } from "@/pages/setup/add-app-wizard/steps/map/session/planner-prompts";

const NO_TABLES_MESSAGES: Record<PlanTableGroup, { filtered: string; unfiltered: string }> = {
    notMigrated: { filtered: "No table the planner left out", unfiltered: "The planner left no tables out." },
    notSelected: { filtered: "No unselected table", unfiltered: "Every table is selected." },
    inCollections: { filtered: "No table in a collection", unfiltered: "No table is in a collection yet." },
};

const GROUP_ROW_HEIGHT_PX = 36;

const TABLE_ROW_HEIGHT_PX = 52;

type TableRow =
    | { kind: "group"; key: string; group: PlanTableGroup; isFirst: boolean }
    | { kind: "table"; key: string; table: PlanTable; isFirst: boolean; isLast: boolean };

/**
 * Every table and where it goes. The planner can only add a table it was started with, so the tables it
 * left out can be handed back to it, while tables that were never selected stay as they are.
 */
export function PlanTablesTab({ filters }: { filters: PlanFilters }) {
    const setPlannerDraft = useSetupWizardStore((state) => state.setPlannerDraft);
    const focusComposer = useFocusPlannerComposer();
    const canCompose = useCanComposeForPlanner();
    const [chosen, setChosen] = useState<ReadonlySet<string>>(new Set());

    // A table the planner has since taken into a collection is no longer one to hand back.
    const notMigrated = new Set(
        filters.tables.filter((table) => table.group === "notMigrated").map((table) => table.label),
    );
    const picked = new Set([...chosen].filter((label) => notMigrated.has(label)));
    const rows = toRows(filters.shownTables);

    const togglePicked = (label: string, isPicked: boolean) => {
        const next = new Set(picked);

        if (isPicked) {
            next.add(label);
        } else {
            next.delete(label);
        }

        setChosen(next);
    };

    const askToAdd = () => {
        const prompt = composeAddTablesPrompt([...picked].sort());
        const draft = useSetupWizardStore.getState().plannerDraft;

        setPlannerDraft(draft.trim() ? `${draft.trimEnd()}\n${prompt}` : prompt);
        setChosen(new Set());
        focusComposer();
    };

    return (
        <>
            <PlannerVirtualList
                items={rows}
                getKey={(row) => row.key}
                estimateSize={(row) => (row.kind === "group" ? GROUP_ROW_HEIGHT_PX : TABLE_ROW_HEIGHT_PX)}
                className="p-4"
                renderItem={(row) =>
                    row.kind === "group" ? (
                        <Heading
                            as="h3"
                            variant="label"
                            className={cn("pb-2 text-muted-foreground", !row.isFirst && "pt-5")}
                        >
                            {PLAN_TABLE_GROUP_LABELS[row.group]}
                        </Heading>
                    ) : (
                        <PlanTableItem
                            row={row}
                            isPicked={picked.has(row.table.label)}
                            onPickedChange={(isPicked) => togglePicked(row.table.label, isPicked)}
                            onShowCollection={filters.showCollection}
                        />
                    )
                }
            >
                {filters.isFiltering && filters.shownTables.length === 0 && (
                    <PlanFilterEmpty
                        message={describeNoTables(filters.filter, filters.tableGroup)}
                        onClearFilters={filters.clearFilters}
                    />
                )}
            </PlannerVirtualList>

            {picked.size > 0 && (
                <div className="flex flex-wrap items-center gap-2 border-t bg-surface2 py-2.5 pr-3 pl-4">
                    <Text variant="label" as="span">
                        {formatCount(picked.size, "table")} chosen
                    </Text>
                    <div className="ml-auto flex gap-1.5">
                        <Button variant="ghost" size="sm" onClick={() => setChosen(new Set())}>
                            Clear
                        </Button>
                        <Button size="sm" disabled={!canCompose} onClick={askToAdd}>
                            <Sparkle data-icon="inline-start" />
                            {picked.size === 1 ? "Ask the planner to add it" : "Ask the planner to add them"}
                        </Button>
                    </div>
                </div>
            )}
        </>
    );
}

function PlanTableItem({
    row,
    isPicked,
    onPickedChange,
    onShowCollection,
}: {
    row: Extract<TableRow, { kind: "table" }>;
    isPicked: boolean;
    onPickedChange: (isPicked: boolean) => void;
    onShowCollection: (collection: string) => void;
}) {
    const { table } = row;

    return (
        <div
            className={cn(
                "flex min-h-13 items-center gap-2.5 border-x border-t bg-card py-2 pr-1 pl-3",
                row.isFirst && "rounded-t-lg",
                row.isLast && "rounded-b-lg border-b",
            )}
        >
            <span className="flex w-4 shrink-0">
                {table.group === "notMigrated" && (
                    <Checkbox
                        checked={isPicked}
                        aria-label={`Choose ${table.label} to add to the plan`}
                        onCheckedChange={(checked) => onPickedChange(checked === true)}
                    />
                )}
            </span>
            <div className="flex min-w-0 flex-1 flex-col gap-0.5">
                <Text as="span" variant="code" className="break-all">
                    {table.label}
                </Text>
                <Text as="span" variant="caption">
                    {table.detail}
                </Text>
            </div>
            {table.group === "inCollections" && <PlanStatusBadge state={table.collection.state} />}
            <span className="flex w-7 shrink-0 items-center justify-center">
                {table.group === "inCollections" && (
                    <Button
                        variant="ghost"
                        size="icon-sm"
                        className="text-muted-foreground"
                        aria-label={`Show ${table.collection.name} in Collections`}
                        onClick={() => onShowCollection(table.collection.name)}
                    >
                        <ChevronRight aria-hidden="true" />
                    </Button>
                )}
            </span>
        </div>
    );
}

/** The tables in their groups, tables without a collection first, so nothing gets forgotten. */
function toRows(tables: PlanTable[]): TableRow[] {
    const groups = (Object.keys(PLAN_TABLE_GROUP_LABELS) as PlanTableGroup[])
        .map((group) => ({ group, tables: tables.filter((table) => table.group === group) }))
        .filter((group) => group.tables.length > 0);

    return groups.flatMap(({ group, tables: groupTables }, groupIndex): TableRow[] => [
        { kind: "group", key: `group:${group}`, group, isFirst: groupIndex === 0 },
        ...groupTables.map(
            (table, index): TableRow => ({
                kind: "table",
                key: `table:${table.label}`,
                table,
                isFirst: index === 0,
                isLast: index === groupTables.length - 1,
            }),
        ),
    ]);
}

function describeNoTables(filter: string, group: PlanTableGroup | typeof ALL_STATES): string {
    const typed = filter.trim();

    if (group === ALL_STATES) {
        return `No table matches "${typed}".`;
    }

    return typed ? `${NO_TABLES_MESSAGES[group].filtered} matches "${typed}".` : NO_TABLES_MESSAGES[group].unfiltered;
}
