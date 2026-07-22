import { api } from "@/api/api";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { type AppFormData, tablesSchema } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { getTableKey } from "@/pages/setup/add-app-wizard/discover-utils";
import { wrapDtoTablesToFormShape } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-dto";
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

    return async () => {
        const { source, aiPrompt } = getValues("map");
        const selectedTables = getValues("verifySchema.tables");
        const store = useSetupWizardStore.getState();

        const appliedMapKey = computeMapKey({ source, aiPrompt, selectedTables });

        // Same inputs as the last generation - keep the (possibly edited) tables.
        if (appliedMapKey === store.appliedMapKey && getValues("mapTables.tables").length > 0) {
            return;
        }

        const tables =
            source === "ai-suggested"
                ? await suggestTables(aiPrompt)
                : scaffoldTables(selectedTables, store.discoverResult);

        setValue("mapTables.tables", tables);
        store.setAppliedMapKey(appliedMapKey);
        store.resetMapTablesUiState();
    };
}

async function suggestTables(aiPrompt: string): Promise<AppFormData["mapTables"]["tables"]> {
    const result = await api.services.setup.suggestCdc({
        intentPrompt: aiPrompt.trim(),
    });

    if (result.status !== "Success" || !result.configuration) {
        throw new Error(result.rationale.filter(Boolean).join("\n") || `AI suggestion failed (${result.status}).`);
    }

    return tablesSchema.parse(wrapDtoTablesToFormShape(result.configuration.tables ?? []));
}
