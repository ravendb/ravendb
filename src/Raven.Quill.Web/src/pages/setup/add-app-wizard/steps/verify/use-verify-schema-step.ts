import { useQueryClient } from "@tanstack/react-query";
import { useFormContext } from "react-hook-form";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import {
    cancelAbandonedSuggestions,
    suggestMapTablesQueryForValues,
} from "@/pages/setup/add-app-wizard/steps/map-tables/suggest-map-tables-query";

/**
 * Warms the AI mapping suggestion while the operator is still choosing a mapping source. The call
 * takes over a minute, and most operators keep the AI default, so the map-tables step usually finds
 * the answer already waiting. Prefetching with the current prompt makes a re-pass through this step
 * a cache no-op instead of a second paid call. Deliberately not awaited - the wizard must advance
 * immediately.
 */
export function useVerifySchemaStep() {
    const queryClient = useQueryClient();
    const { getValues } = useFormContext<AppFormData>();

    return () => {
        // After an import or a manual mapping choice the suggestion could never be consumed, so the
        // (expensive) call must not be warmed up.
        if (getValues("map.source") !== "ai-suggested") {
            cancelAbandonedSuggestions(queryClient);
            return;
        }

        const { discoverResult } = useSetupWizardStore.getState();
        const query = suggestMapTablesQueryForValues(getValues(), discoverResult);

        // Re-entering this step with different tables or a different prompt leaves the previous pass
        // still running.
        cancelAbandonedSuggestions(queryClient, query.queryKey);

        void queryClient.prefetchQuery(query);
    };
}
