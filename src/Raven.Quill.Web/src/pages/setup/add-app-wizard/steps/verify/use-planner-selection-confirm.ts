import { useFormContext, useWatch } from "react-hook-form";
import type { WizardConfirmNext } from "@/components/form/wizard/form-wizard";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { haveSameTables } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-utils";

export function usePlannerSelectionConfirm(): WizardConfirmNext {
    const { control } = useFormContext<AppFormData>();
    const tables = useWatch({ control, name: "verifySchema.tables" });
    const hasMapping = useWatch({ control, name: "mapTables.tables", compute: (mapped) => mapped.length > 0 });

    // The planner reads the schema when it starts, so a session still opening is just as tied to its tables.
    const hasSession = useSetupWizardStore((state) => state.plannerTurns.length > 0);
    const plannerSelectedTables = useSetupWizardStore((state) => state.plannerSelectedTables);
    const startPlannerOver = useSetupWizardStore((state) => state.startPlannerOver);

    const isSameSelection = haveSameTables(tables, plannerSelectedTables ?? []);
    // The planner starts on its own only while the form holds no mapping; one applied earlier stays until replaced.
    const consequence = hasMapping
        ? "Continuing discards it. The next step keeps your current mapping, and you can start the planner there."
        : "Continuing discards it, and you'll start a new session on the next step.";

    return {
        isRequired: hasSession && !isSameSelection,
        title: "Start the planner over?",
        description: `The planner session was built on a different table selection. ${consequence}`,
        confirmLabel: "Start over",
        onConfirm: startPlannerOver,
    };
}
