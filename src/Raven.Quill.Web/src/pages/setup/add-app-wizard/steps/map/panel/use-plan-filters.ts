import { useState } from "react";
import { useFormContext, useWatch } from "react-hook-form";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { indexDiscovery } from "@/pages/setup/add-app-wizard/steps/map/session/planner-document";
import {
    buildPlanTables,
    createNameMatcher,
    type PlanCollection,
    type PlanCollectionState,
    type PlanTable,
    type PlanTableGroup,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-plan";

export const ALL_STATES = "all";

export type PlanTab = "collections" | "tables";

/** Every table and where it goes. It depends on the plan and the schema only, so no filter rebuilds it. */
export function usePlanTables(collections: PlanCollection[]): PlanTable[] {
    const { control } = useFormContext<AppFormData>();
    const selectedTables = useWatch({ control, name: "verifySchema.tables" });
    const proposal = useSetupWizardStore((state) => state.plannerProposal);
    const discoverResult = useSetupWizardStore((state) => state.discoverResult);

    return buildPlanTables(indexDiscovery(discoverResult), selectedTables, collections, proposal);
}

/**
 * What the plan panel shows. The name filter matches collections and their source tables alike, a
 * focused collection shows that one alone, and the state chips narrow what matched - they only change
 * what is shown, never what gets applied.
 */
export function usePlanFilters(collections: PlanCollection[], tables: PlanTable[]) {
    const [tab, setTab] = useState<PlanTab>("collections");
    const [filter, setFilter] = useState("");
    const [focusedCollection, setFocusedCollection] = useState<string | null>(null);
    const [collectionState, setCollectionState] = useState<PlanCollectionState | typeof ALL_STATES>(ALL_STATES);
    const [tableGroup, setTableGroup] = useState<PlanTableGroup | typeof ALL_STATES>(ALL_STATES);

    const matchesName = createNameMatcher(filter);
    const matchingCollections =
        focusedCollection === null
            ? collections.filter((collection) => matchesName(collection.searchKey))
            : collections.filter((collection) => collection.name === focusedCollection);
    const matchingTables = tables.filter((table) => matchesName(table.searchKey));
    const isTablesTab = tab === "tables";

    return {
        tab,
        setTab,
        filter,
        setFilter: (value: string) => {
            setFocusedCollection(null);
            setFilter(value);
        },
        focusedCollection,
        collectionState,
        setCollectionState,
        tableGroup,
        setTableGroup,
        tables,
        matchingCollections,
        matchingTables,
        shownCollections: matchingCollections.filter(
            (collection) => collectionState === ALL_STATES || collection.state === collectionState,
        ),
        shownTables: matchingTables.filter((table) => tableGroup === ALL_STATES || table.group === tableGroup),
        isFiltering:
            filter.trim() !== "" ||
            (!isTablesTab && focusedCollection !== null) ||
            (isTablesTab ? tableGroup : collectionState) !== ALL_STATES,
        clearFilters: () => {
            setFilter("");
            setFocusedCollection(null);
            setCollectionState(ALL_STATES);
            setTableGroup(ALL_STATES);
        },
        showTables: () => {
            setTab("tables");
            setTableGroup(ALL_STATES);
        },
        showCollection: (collection: string) => {
            setTab("collections");
            setFilter("");
            setFocusedCollection(collection);
            setCollectionState(ALL_STATES);
        },
    };
}

export type PlanFilters = ReturnType<typeof usePlanFilters>;
