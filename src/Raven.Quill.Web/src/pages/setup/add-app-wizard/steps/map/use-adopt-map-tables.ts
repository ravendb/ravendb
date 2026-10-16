import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { tablesSchema, type AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { useApplyMapTables } from "@/pages/setup/add-app-wizard/steps/map-tables/use-apply-map-tables";
import type { MapOrigin } from "@/pages/setup/add-app-wizard/steps/map/map-source-options";

export function useAdoptMapTables() {
    const applyMapTables = useApplyMapTables();
    const setMapOrigin = useSetupWizardStore((state) => state.setMapOrigin);
    const resetMapTablesUiState = useSetupWizardStore((state) => state.resetMapTablesUiState);

    return (tables: AppFormData["mapTables"]["tables"], origin: MapOrigin) => {
        applyMapTables(tables);
        setMapOrigin(origin);
        resetMapTablesUiState();
    };
}

export function parsePlannerTables(tables: unknown[]): AppFormData["mapTables"]["tables"] {
    return tablesSchema.parse(tables);
}
