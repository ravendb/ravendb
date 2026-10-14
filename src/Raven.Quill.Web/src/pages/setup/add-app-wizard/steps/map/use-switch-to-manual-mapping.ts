import { useFormContext } from "react-hook-form";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { type AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { addUnmappedSelectedTables } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-utils";
import { useAdoptMapTables } from "@/pages/setup/add-app-wizard/steps/map/use-adopt-map-tables";

/**
 * Leaves the planner for the editor. A mapping the form already holds - imported, or carried over from
 * an earlier visit - is kept whole, and the selected tables it does not map yet are scaffolded.
 */
export function useSwitchToManualMapping() {
    const { getValues } = useFormContext<AppFormData>();
    const adoptTables = useAdoptMapTables();
    const stopPlannerTurn = useSetupWizardStore((state) => state.stopPlannerTurn);
    const discoverResult = useSetupWizardStore((state) => state.discoverResult);

    return () => {
        stopPlannerTurn();
        adoptTables(
            addUnmappedSelectedTables(getValues("mapTables.tables"), getValues("verifySchema.tables"), discoverResult),
            { source: "manual" },
        );
    };
}
