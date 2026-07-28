import { useQueryClient } from "@tanstack/react-query";
import { useFormContext } from "react-hook-form";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import {
    computeDiscoveredSchemaKey,
    suggestMapTablesQuery,
} from "@/pages/setup/add-app-wizard/steps/map-tables/suggest-map-tables-query";

/**
 * Warms the AI mapping suggestion while the operator is still choosing a mapping source. The call
 * takes over a minute, and the empty (default) prompt is what most operators end up sending, so the
 * map-tables step usually finds the answer already waiting. Deliberately not awaited - the wizard
 * must advance immediately.
 */
export function useVerifySchemaStep() {
    const queryClient = useQueryClient();
    const { getValues } = useFormContext<AppFormData>();

    return () => {
        const { discoverResult } = useSetupWizardStore.getState();

        void queryClient.prefetchQuery(
            suggestMapTablesQuery({
                slug: getValues("externalConnection").slug,
                discoveredSchemaKey: computeDiscoveredSchemaKey(discoverResult),
                intentPrompt: "",
                selectedTables: getValues("verifySchema.tables"),
            }),
        );
    };
}
