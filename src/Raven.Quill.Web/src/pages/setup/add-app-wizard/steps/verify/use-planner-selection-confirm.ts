import { useFormContext, useWatch } from "react-hook-form";
import type { WizardConfirmNext } from "@/components/form/wizard/form-wizard";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { haveSameTables } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-utils";
import { selectHasSession } from "@/pages/setup/add-app-wizard/steps/map/session/planner-state";

export function usePlannerSelectionConfirm(): WizardConfirmNext {
    const { control } = useFormContext<AppFormData>();
    const tables = useWatch({ control, name: "verifySchema.tables" });

    // The planner reads the schema when it starts, so a session still opening is just as tied to its tables.
    const hasSession = useSetupWizardStore(selectHasSession);
    const plannerSelectedTables = useSetupWizardStore((state) => state.plannerSelectedTables);
    const startPlannerOver = useSetupWizardStore((state) => state.startPlannerOver);

    return {
        isRequired: hasSession && !haveSameTables(tables, plannerSelectedTables ?? []),
        title: "Start the planner over?",
        description:
            "The plan was made from a different table selection. Continuing stops the planner and discards the plan, and you start over on the next step.",
        confirmLabel: "Start over",
        onConfirm: startPlannerOver,
    };
}
