import { useFormContext, useWatch } from "react-hook-form";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { type AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";

/**
 * Why the planner step is holding Next back. A mapping already in the form - scaffolded manually,
 * imported, or applied on an earlier visit - is a complete answer on its own, so the planner only
 * blocks when it is the thing being waited on.
 */
export function usePlannerNextBlock(): { isNextDisabled: boolean; nextDisabledReason?: string } {
    const { control } = useFormContext<AppFormData>();
    const tables = useWatch({ control, name: "mapTables.tables" });

    const isStreaming = useSetupWizardStore((state) => state.isPlannerStreaming);
    const collections = useSetupWizardStore((state) => state.plannerCollections);
    const deselected = useSetupWizardStore((state) => state.plannerDeselected);
    const hasQuestions = useSetupWizardStore((state) => state.plannerQuestions.length > 0);

    if (isStreaming) {
        return { isNextDisabled: true, nextDisabledReason: "The planner is still working." };
    }

    if (hasQuestions) {
        return {
            isNextDisabled: true,
            nextDisabledReason: "Answer the planner's questions, or skip them to accept its recommendations.",
        };
    }

    const registered = Object.values(collections).filter((collection) => collection.status !== "rejected");

    if (registered.length === 0) {
        return tables.length > 0
            ? { isNextDisabled: false }
            : {
                  isNextDisabled: true,
                  nextDisabledReason: "Start a session and let the planner register a collection, or use manual mapping.",
              };
    }

    const hasSelection = registered.some((collection) => !deselected[collection.collection]);

    return hasSelection
        ? { isNextDisabled: false }
        : { isNextDisabled: true, nextDisabledReason: "Select at least one collection to carry forward." };
}
