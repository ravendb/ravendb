import { useFormContext, useWatch } from "react-hook-form";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { useVerifyCdcState } from "@/pages/setup/add-app-wizard/steps/verify/use-verify-cdc-step";

/** The CDC dry run keyed to the verify step's table selection, for the step and its button. */
export function useVerifySchemaCdcState() {
    const { control } = useFormContext<AppFormData>();
    const selectedTables = useWatch({ control, name: "verifySchema.tables" });
    const state = useVerifyCdcState(selectedTables);

    return { selectedTables, ...state };
}
