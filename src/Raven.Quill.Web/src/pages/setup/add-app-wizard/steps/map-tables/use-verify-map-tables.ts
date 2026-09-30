import { useFormContext, useWatch } from "react-hook-form";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { collectMappedSourceTables } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-utils";
import { useVerifyCdcState } from "@/pages/setup/add-app-wizard/steps/verify/use-verify-cdc-step";

/** The CDC dry run keyed to every source table the current mapping captures, for the step and its
 * button. When the mapping covers exactly the verify step's selection, this resolves from the entry
 * that step already paid for. */
export function useVerifyMapTablesState() {
    const { control } = useFormContext<AppFormData>();
    const tables = useWatch({ control, name: "mapTables.tables" });
    const selectedTables = collectMappedSourceTables(tables);
    const state = useVerifyCdcState(selectedTables);

    return { selectedTables, ...state };
}
