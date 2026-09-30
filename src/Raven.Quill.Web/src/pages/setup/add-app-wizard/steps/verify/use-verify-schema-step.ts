import { useQueryClient } from "@tanstack/react-query";
import { useFormContext } from "react-hook-form";
import { useAiConsent } from "@/components/ai-consent/use-ai-consent";
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
    const consent = useAiConsent();

    return () => {
        // After an import or a manual mapping choice the suggestion could never be consumed, so the
        // (expensive) call must not be warmed up. Nor while the AI service is out of reach or still
        // waiting on consent. A check that has not settled yet is warmed anyway: a refused call costs
        // nothing, and recording the consent invalidates it.
        if (
            getValues("map.source") !== "ai-suggested" ||
            consent.isConsentRequired ||
            consent.unavailableReason !== undefined
        ) {
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
