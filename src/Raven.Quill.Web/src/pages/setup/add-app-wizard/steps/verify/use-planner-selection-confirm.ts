import { useFormContext, useWatch } from "react-hook-form";
import type { WizardConfirmNext } from "@/components/form/wizard/form-wizard";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { haveSameTables } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-utils";

export function usePlannerSelectionConfirm(): WizardConfirmNext {
    const { control } = useFormContext<AppFormData>();
    const tables = useWatch({ control, name: "verifySchema.tables" });

    const hasConversation = useSetupWizardStore((state) => state.plannerConversationId !== null);
    const plannerSelectedTables = useSetupWizardStore((state) => state.plannerSelectedTables);
    const resetPlannerState = useSetupWizardStore((state) => state.resetPlannerState);

    return {
        isRequired: hasConversation && !haveSameTables(tables, plannerSelectedTables ?? []),
        title: "Start the planner over?",
        description:
            "The planner session was built on a different table selection. Continuing discards it, and you'll " +
            "start a new session on the next step.",
        confirmLabel: "Start over",
        onConfirm: resetPlannerState,
    };
}
