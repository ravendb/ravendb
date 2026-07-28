import { useLayoutEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { useFormContext, useWatch } from "react-hook-form";
import { getFetchStartedAt } from "@/lib/query-fetch-start";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { computeMapKey } from "@/pages/setup/add-app-wizard/steps/map/use-map-schema-step";
import {
    computeDiscoveredSchemaKey,
    suggestMapTablesQuery,
} from "@/pages/setup/add-app-wizard/steps/map-tables/suggest-map-tables-query";
import { useApplyMapTables } from "@/pages/setup/add-app-wizard/steps/map-tables/use-apply-map-tables";

type SuggestedMapTables = {
    isSuggesting: boolean;
    startedAt: number | undefined;
    error: Error | null;
    retry: () => void;
};

/** The suggestion query for the mapping inputs currently held by the form. */
function useCurrentSuggestQuery() {
    const { getValues } = useFormContext<AppFormData>();
    const discoverResult = useSetupWizardStore((state) => state.discoverResult);

    return suggestMapTablesQuery({
        slug: getValues("externalConnection").slug,
        discoveredSchemaKey: computeDiscoveredSchemaKey(discoverResult),
        intentPrompt: getValues("map.aiPrompt").trim(),
        selectedTables: getValues("verifySchema.tables"),
    });
}

/**
 * Loads the AI mapping suggestion into the form when the operator asked for one. Usually resolves
 * from the entry the verify step prefetched; otherwise the caller renders progress while it runs.
 */
export function useSuggestedMapTables(): SuggestedMapTables {
    const { getValues } = useFormContext<AppFormData>();
    const applyMapTables = useApplyMapTables();
    const appliedMapKey = useSetupWizardStore((state) => state.appliedMapKey);
    const currentSuggestQuery = useCurrentSuggestQuery();

    const { source, aiPrompt } = getValues("map");
    const selectedTables = getValues("verifySchema.tables");
    const mapKey = computeMapKey({ source, aiPrompt, selectedTables });
    const isApplied = appliedMapKey === mapKey && getValues("mapTables.tables").length > 0;
    const isSuggestionNeeded = source === "ai-suggested" && !isApplied;

    const query = useQuery({
        ...currentSuggestQuery,
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

    // A retry in flight reads as progress, not as the failure it is retrying.
    const error = isSuggestionNeeded && !query.isFetching ? query.error : null;

    return {
        isSuggesting: isSuggestionNeeded && !error,
        startedAt: getFetchStartedAt(currentSuggestQuery.queryKey),
        error,
        retry: () => void query.refetch(),
    };
}

/**
 * Whether the wizard should hold "Next" back: while the suggestion for the current inputs is in
 * flight there are no tables to advance with, and after it failed advancing would silently submit
 * whatever mapping the form held before. Matches the exact query key so abandoned requests for
 * other inputs never block the step, and never starts a fetch of its own.
 */
export function useIsMapTablesNextDisabled(): boolean {
    const source = useWatch<AppFormData, "map.source">({ name: "map.source" });
    const currentSuggestQuery = useCurrentSuggestQuery();
    const query = useQuery({ ...currentSuggestQuery, enabled: false });

    return source === "ai-suggested" && (query.isFetching || query.isError);
}
