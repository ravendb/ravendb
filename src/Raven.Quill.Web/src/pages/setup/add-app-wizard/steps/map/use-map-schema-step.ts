import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { type AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { getTableKey } from "@/pages/setup/add-app-wizard/discover-utils";
import { scaffoldTables } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-utils";
import { useFormContext } from "react-hook-form";

export function computeMapKey(map: {
    source: AppFormData["map"]["source"];
    aiPrompt: string;
    selectedTables: AppFormData["verifySchema"]["tables"];
}): string {
    return JSON.stringify({
        source: map.source,
        aiPrompt: map.source === "ai-suggested" ? map.aiPrompt.trim() : "",
        selectedTables: map.selectedTables.map(getTableKey).sort(),
    });
}

export function useMapSchemaStep() {
    const { getValues, setValue } = useFormContext<AppFormData>();

    return () => {
        const { source, aiPrompt } = getValues("map");

        // The AI suggestion is fetched by the map-tables step itself, so it can render its own
        // progress for the minute or more the call takes instead of freezing "Next" here.
        if (source === "ai-suggested") {
            return;
        }

        const selectedTables = getValues("verifySchema.tables");
        const store = useSetupWizardStore.getState();

        const appliedMapKey = computeMapKey({ source, aiPrompt, selectedTables });

        // Same inputs as the last generation - keep the (possibly edited) tables.
        if (appliedMapKey === store.appliedMapKey && getValues("mapTables.tables").length > 0) {
            return;
        }

        setValue("mapTables.tables", scaffoldTables(selectedTables, store.discoverResult));
        store.setAppliedMapKey(appliedMapKey);
        store.resetMapTablesUiState();
    };
}
