import { useLayoutEffect } from "react";
import { useIsFetching, useQuery } from "@tanstack/react-query";
import { useFormContext, useWatch } from "react-hook-form";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { computeMapKey } from "@/pages/setup/add-app-wizard/steps/map/use-map-schema-step";
import {
    computeDiscoveredSchemaKey,
    SUGGEST_MAP_TABLES_QUERY_KEY,
    suggestMapTablesQuery,
} from "@/pages/setup/add-app-wizard/steps/map-tables/suggest-map-tables-query";
import { useApplyMapTables } from "@/pages/setup/add-app-wizard/steps/map-tables/use-apply-map-tables";

type SuggestedMapTables = {
    isSuggesting: boolean;
    error: Error | null;
    retry: () => void;
};

/**
 * Loads the AI mapping suggestion into the form when the operator asked for one. Usually resolves
 * from the entry the verify step prefetched; otherwise the caller renders progress while it runs.
 */
export function useSuggestedMapTables(): SuggestedMapTables {
    const { getValues } = useFormContext<AppFormData>();
    const applyMapTables = useApplyMapTables();
    const appliedMapKey = useSetupWizardStore((state) => state.appliedMapKey);
    const discoverResult = useSetupWizardStore((state) => state.discoverResult);

    const { source, aiPrompt } = getValues("map");
    const selectedTables = getValues("verifySchema.tables");
    const mapKey = computeMapKey({ source, aiPrompt, selectedTables });
    const isApplied = appliedMapKey === mapKey && getValues("mapTables.tables").length > 0;
    const isSuggestionNeeded = source === "ai-suggested" && !isApplied;

    const query = useQuery({
        ...suggestMapTablesQuery({
            slug: getValues("externalConnection").slug,
            discoveredSchemaKey: computeDiscoveredSchemaKey(discoverResult),
            intentPrompt: aiPrompt.trim(),
            selectedTables,
        }),
        enabled: isSuggestionNeeded,
    });

    const suggestedTables = query.data;

    // The suggestion arrives from the server after this step is already on screen, so writing it into
    // the form is a genuine sync step. The applied key keeps it to once per set of mapping inputs, so
    // edits survive navigating back and forth. It runs before paint so a prefetched suggestion, which
    // is already in the cache on the first render, never flashes the progress skeleton.
    useLayoutEffect(() => {
        if (!suggestedTables || isApplied) {
            return;
        }

        const store = useSetupWizardStore.getState();

        store.resetMapTablesUiState();
        applyMapTables(suggestedTables);
        store.setAppliedMapKey(mapKey);
    }, [applyMapTables, isApplied, mapKey, suggestedTables]);

    return {
        isSuggesting: isSuggestionNeeded && (query.isFetching || !query.isError),
        error: isSuggestionNeeded ? query.error : null,
        retry: () => void query.refetch(),
    };
}

/**
 * Whether the wizard should hold "Next" back: with no tables in the form yet, advancing could only
 * fail validation. Observes the cache instead of the query so the step definitions never start a
 * fetch of their own.
 */
export function useIsSuggestingMapTables(): boolean {
    const source = useWatch<AppFormData, "map.source">({ name: "map.source" });
    const isFetching = useIsFetching({ queryKey: SUGGEST_MAP_TABLES_QUERY_KEY }) > 0;

    return source === "ai-suggested" && isFetching;
}
