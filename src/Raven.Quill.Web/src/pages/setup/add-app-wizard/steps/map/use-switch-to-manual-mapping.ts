import { useFormContext } from "react-hook-form";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { type AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { resolveConnectionDefaultSchema } from "@/pages/setup/add-app-wizard/discover-utils";
import { addUnmappedSelectedTables } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-utils";
import { useAdoptMapTables } from "@/pages/setup/add-app-wizard/steps/map/use-adopt-map-tables";

export function useSwitchToManualMapping() {
    const { getValues } = useFormContext<AppFormData>();
    const adoptTables = useAdoptMapTables();
    const stopPlannerTurns = useSetupWizardStore((state) => state.stopPlannerTurns);
    const discoverResult = useSetupWizardStore((state) => state.discoverResult);

    return () => {
        void stopPlannerTurns();
        adoptTables(
            addUnmappedSelectedTables(
                getValues("mapTables.tables"),
                getValues("verifySchema.tables"),
                discoverResult,
                resolveConnectionDefaultSchema(getValues("externalConnection")),
            ),
            { source: "manual" },
        );
    };
}
