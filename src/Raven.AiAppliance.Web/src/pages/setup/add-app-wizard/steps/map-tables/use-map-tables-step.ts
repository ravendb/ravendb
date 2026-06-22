import { api } from "@/api/api";
import { getSourceTableLabel } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-utils";
import { mapFormTablesToDto } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-dto";
import { parseRawTablesToForm } from "@/pages/setup/add-app-wizard/steps/map-tables/raw-tables";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { useFormContext } from "react-hook-form";

export function computeMapTablesKey(tables: AppFormData["mapTables"]["tables"]): string {
    return JSON.stringify(mapFormTablesToDto(tables));
}

export function useMapTablesStep() {
    const { getValues, setValue } = useFormContext<AppFormData>();

    return async () => {
        const store = useSetupWizardStore.getState();

        // If the raw JSON editor is still open, apply its edits before mapping. A parse/validation
        // error throws here so the wizard blocks "Next" and surfaces the message instead of advancing
        // with stale form data.
        if (store.isMapTablesRawView) {
            setValue("mapTables.tables", parseRawTablesToForm(store.mapTablesRawContent), {
                shouldDirty: true,
                shouldValidate: true,
            });
            store.closeMapTablesRawView();
        }

        const formTables = getValues("mapTables.tables");
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
