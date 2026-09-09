import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { useFormContext } from "react-hook-form";

/**
 * Replaces the whole mapTables.tables array and validates it once. Passing shouldValidate or
 * shouldDirty to setValue here makes RHF recurse into every leaf field and run full-form
 * validation / dirty diffing per leaf - O(n^2), multiple seconds with 100+ tables.
 */
export function useApplyMapTables() {
    const { setValue, trigger } = useFormContext<AppFormData>();

    return (tables: AppFormData["mapTables"]["tables"]) => {
        setValue("mapTables.tables", tables);
        void trigger("mapTables.tables");
    };
}
