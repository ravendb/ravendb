import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { tablesSchema, type AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { useApplyMapTables } from "@/pages/setup/add-app-wizard/steps/map-tables/use-apply-map-tables";
import type { MapOrigin } from "@/pages/setup/add-app-wizard/steps/map/map-source-options";

export const MANUAL_MAP_ORIGIN = "manual";

export const IMPORTED_MAP_ORIGIN = "import";

export function useAdoptMapTables() {
    const applyMapTables = useApplyMapTables();

    return (tables: AppFormData["mapTables"]["tables"], origin: MapOrigin) => {
        const store = useSetupWizardStore.getState();

        applyMapTables(tables);
        store.setMapOrigin(origin);
        store.resetMapTablesUiState();
    };
}

export function parsePlannerTables(tables: unknown[]): AppFormData["mapTables"]["tables"] {
    return tablesSchema.parse(tables);
}
