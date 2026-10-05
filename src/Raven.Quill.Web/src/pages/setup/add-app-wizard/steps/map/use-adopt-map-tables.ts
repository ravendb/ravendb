import { useFormContext } from "react-hook-form";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { tablesSchema, type AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { computeSourceKey } from "@/pages/setup/add-app-wizard/steps/connect/use-connect-source-step";
import { computeMapKey } from "@/pages/setup/add-app-wizard/steps/map/use-map-schema-step";
import { useApplyMapTables } from "@/pages/setup/add-app-wizard/steps/map-tables/use-apply-map-tables";

/**
 * Hands a mapping to the map-tables editor, whatever produced it.
 *
 * Pinning `map.source` to "manual" is load-bearing: it is what stops the editor firing a suggestion
 * query of its own, and claiming the applied key is what makes it treat this mapping as the answer
 * for the current inputs instead of regenerating over it.
 */
export function useAdoptMapTables() {
    const { getValues, setValue } = useFormContext<AppFormData>();
    const applyMapTables = useApplyMapTables();

    return (tables: AppFormData["mapTables"]["tables"]) => {
        const store = useSetupWizardStore.getState();

        setValue("map.source", "manual");
        applyMapTables(tables);
        store.setAppliedMapKey(
            computeMapKey({
                sourceKey: computeSourceKey(getValues("externalConnection")),
                source: "manual",
                aiPrompt: "",
                selectedTables: getValues("verifySchema").tables,
            }),
        );
        store.resetMapTablesUiState();
    };
}

/** Parses a configuration returned by the planner into the editor's form shape. */
export function parsePlannerTables(tables: unknown[]): AppFormData["mapTables"]["tables"] {
    return tablesSchema.parse(tables);
}
