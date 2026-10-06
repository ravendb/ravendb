import { useFormContext, useWatch } from "react-hook-form";
import { useSetupWizardStore, type PlannerCollection } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { type AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";

type PlannerNextBlock = { isNextDisabled: boolean; nextDisabledReason?: string };

/**
 * Why the planner step is holding Next back. A mapping already in the form - scaffolded manually,
 * imported, or applied on an earlier visit - is a complete answer on its own, so the planner only
 * blocks when it is the thing being waited on.
 */
export function usePlannerNextBlock(): PlannerNextBlock {
    const { control } = useFormContext<AppFormData>();
    const tables = useWatch({ control, name: "mapTables.tables" });

    const isStreaming = useSetupWizardStore((state) => state.isPlannerStreaming);
    const conversationId = useSetupWizardStore((state) => state.plannerConversationId);
    const collections = useSetupWizardStore((state) => state.plannerCollections);
    const deselected = useSetupWizardStore((state) => state.plannerDeselected);
    const hasQuestions = useSetupWizardStore((state) => state.plannerQuestions.length > 0);

    return getPlannerNextBlock({
        isStreaming,
        hasQuestions,
        hasConversation: conversationId !== null,
        hasMappedTables: tables.length > 0,
        collections: Object.values(collections),
        deselected,
    });
}

export function getPlannerNextBlock({
    isStreaming,
    hasQuestions,
    hasConversation,
    hasMappedTables,
    collections,
    deselected,
}: {
    isStreaming: boolean;
    hasQuestions: boolean;
    hasConversation: boolean;
    hasMappedTables: boolean;
    collections: PlannerCollection[];
    deselected: Record<string, boolean>;
}): PlannerNextBlock {
    if (isStreaming) {
        return { isNextDisabled: true, nextDisabledReason: "The planner is still working." };
    }

    if (hasQuestions) {
        return {
            isNextDisabled: true,
            nextDisabledReason: "Answer the planner's questions, or skip them to accept its recommendations.",
        };
    }

    const registered = collections.filter((collection) => collection.status !== "rejected");

    if (registered.length === 0) {
        return hasMappedTables
            ? { isNextDisabled: false }
            : {
                  isNextDisabled: true,
                  nextDisabledReason:
                      "Start a session and let the planner register a collection, or use manual mapping.",
              };
    }

    if (!hasConversation) {
        return { isNextDisabled: true, nextDisabledReason: "The planner session did not finish. Start over." };
    }

    const hasSelection = registered.some((collection) => !deselected[collection.collection]);

    return hasSelection
        ? { isNextDisabled: false }
        : { isNextDisabled: true, nextDisabledReason: "Select at least one collection to carry forward." };
}
