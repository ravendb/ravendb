import { useQueryClient } from "@tanstack/react-query";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { type AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { getTableKey } from "@/pages/setup/add-app-wizard/discover-utils";
import { computeSourceKey } from "@/pages/setup/add-app-wizard/steps/connect/use-connect-source-step";
import { scaffoldTables } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-utils";
import {
    cancelAbandonedSuggestions,
    suggestMapTablesQueryForValues,
} from "@/pages/setup/add-app-wizard/steps/map-tables/suggest-map-tables-query";
import { useFormContext } from "react-hook-form";

export function computeMapKey(map: {
    sourceKey: string;
    source: AppFormData["map"]["source"];
    aiPrompt: string;
    selectedTables: AppFormData["verifySchema"]["tables"];
}): string {
    return JSON.stringify({
        sourceKey: map.sourceKey,
        source: map.source,
        aiPrompt: map.source === "ai-suggested" ? map.aiPrompt.trim() : "",
        selectedTables: map.selectedTables.map(getTableKey).sort(),
    });
}

export function useMapSchemaStep() {
    const queryClient = useQueryClient();
    const { getValues, setValue } = useFormContext<AppFormData>();

    return () => {
        const values = getValues();
        const { source, aiPrompt } = values.map;
        const store = useSetupWizardStore.getState();

        // Editing the prompt here (or dropping the AI mapping altogether) makes the suggestion the
        // verify step prefetched an answer to a question nobody asked any more.
        cancelAbandonedSuggestions(
            queryClient,
            source === "ai-suggested"
                ? suggestMapTablesQueryForValues(values, store.discoverResult).queryKey
                : undefined,
        );

        // The AI suggestion is fetched by the map-tables step itself, so it can render its own
        // progress for the minute or more the call takes instead of freezing "Next" here.
        if (source === "ai-suggested") {
            return;
        }

        const selectedTables = values.verifySchema.tables;
        const appliedMapKey = computeMapKey({
            sourceKey: computeSourceKey(values.externalConnection),
            source,
            aiPrompt,
            selectedTables,
        });

        // Same inputs as the last generation - keep the (possibly edited) tables.
        if (appliedMapKey === store.appliedMapKey && getValues("mapTables.tables").length > 0) {
            return;
        }

        setValue("mapTables.tables", scaffoldTables(selectedTables, store.discoverResult));
        store.setAppliedMapKey(appliedMapKey);
        store.resetMapTablesUiState();
    };
}
