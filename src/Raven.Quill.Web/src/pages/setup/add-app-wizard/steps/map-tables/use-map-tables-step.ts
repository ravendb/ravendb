import { useQueryClient } from "@tanstack/react-query";
import { api } from "@/api/api";
import {
    collectMappedSourceTables,
    getSourceTableLabel,
} from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-utils";
import { fetchVerifyCdc } from "@/pages/setup/add-app-wizard/steps/verify/verify-cdc-query";
import { mapFormTablesToDto } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-dto";
import { parseRawTablesToForm } from "@/pages/setup/add-app-wizard/steps/map-tables/raw-tables";
import { useApplyMapTables } from "@/pages/setup/add-app-wizard/steps/map-tables/use-apply-map-tables";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import type { WizardProgress } from "@/components/form/wizard/form-wizard";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { computeConnectKey } from "@/pages/setup/add-app-wizard/steps/connect/use-connect-source-step";
import { useFormContext } from "react-hook-form";

// The server stores the mapping on the state document of the slug it was posted for, so the call has
// to run again for a new slug (or a new source) even when the tables themselves did not change.
function computeMapTablesKey(connectKey: string, tables: AppFormData["mapTables"]["tables"]): string {
    return JSON.stringify({ connectKey, tables: mapFormTablesToDto(tables) });
}

export function useMapTablesStep() {
    const queryClient = useQueryClient();
    const { getValues, setValue } = useFormContext<AppFormData>();
    const applyMapTables = useApplyMapTables();

    return async (progress: WizardProgress) => {
        const store = useSetupWizardStore.getState();

        // If the raw JSON editor is still open, apply its edits before mapping. A parse/validation
        // error throws here so the wizard blocks "Next" and surfaces the message instead of advancing
        // with stale form data.
        if (store.isMapTablesRawView) {
            applyMapTables(parseRawTablesToForm(store.mapTablesRawContent));
            store.closeMapTablesRawView();
        }

        const connection = getValues("externalConnection");
        const formTables = getValues("mapTables.tables");

        const mappedSourceTables = collectMappedSourceTables(formTables);

        if (mappedSourceTables.length > 0) {
            // Serves the cached pass when this table set was already verified, here or from the
            // "Verify tables" button - or from the verify step, when the mapping covers exactly
            // its selection.
            await fetchVerifyCdc(
                queryClient,
                { connectKey: store.connectKey, slug: connection.slug, selectedTables: mappedSourceTables },
                progress,
                "Verifying tables...",
            );
        }

        const mapTablesKey = computeMapTablesKey(computeConnectKey(connection), formTables);

        // Read again: the dry run above awaited, so the snapshot taken before it can be stale.
        if (mapTablesKey === useSetupWizardStore.getState().mapTablesKey) {
            return;
        }

        progress.report("Applying mapping...");
        await api.services.setup.map({
            tables: mapFormTablesToDto(formTables),
            slug: connection.slug,
        });

        const firstTable = formTables[0];
        setValue("preview.table", getSourceTableLabel(firstTable) ?? "");
        store.setMapTablesKey(mapTablesKey);
    };
}
