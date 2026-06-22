import { api } from "@/api/api";
import { getSourceTableLabel } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-utils";
import { mapFormTablesToDto } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-dto";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { useFormContext } from "react-hook-form";

export function computeMapTablesKey(tables: AppFormData["mapTables"]["tables"]): string {
    return JSON.stringify(mapFormTablesToDto(tables));
}

export function useMapTablesStep() {
    const { getValues, setValue } = useFormContext<AppFormData>();

    return async () => {
        const formTables = getValues("mapTables.tables");
        const store = useSetupWizardStore.getState();
        const mapTablesKey = computeMapTablesKey(formTables);

        if (mapTablesKey === store.mapTablesKey) {
            return;
        }

        await api.services.setup.map({
            tables: mapFormTablesToDto(formTables),
        });

        const firstTable = formTables[0];
        setValue("preview.table", getSourceTableLabel(firstTable) ?? "");
        store.setMapTablesKey(mapTablesKey);
    };
}
