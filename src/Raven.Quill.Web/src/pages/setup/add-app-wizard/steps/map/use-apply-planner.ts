import { useFormContext } from "react-hook-form";
import { api } from "@/api/api";
import { WizardStepError } from "@/components/form/wizard/wizard-step-error";
import { useSetupWizardStore, type PlannerCollection } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { type AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { wrapDtoTablesToFormShape } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-dto";
import { parsePlannerTables, useAdoptMapTables } from "@/pages/setup/add-app-wizard/steps/map/use-adopt-map-tables";

/**
 * Runs as the step's `beforeNext`: the plan is assembled server-side, validated as a whole, and
 * handed to the editor. Throwing here keeps the wizard on this step, which is the point - a plan
 * that does not assemble must not quietly advance.
 */
export function useApplyPlanner() {
    const { getValues } = useFormContext<AppFormData>();
    const adoptTables = useAdoptMapTables();

    return async () => {
        const store = useSetupWizardStore.getState();
        const conversationId = store.plannerConversationId;

        // Nothing registered means the operator reached Next through the manual path, which has
        // already written the mapping itself.
        if (conversationId === null || Object.keys(store.plannerCollections).length === 0) {
            return;
        }

        const selectedCollections = Object.values(store.plannerCollections)
            .filter((collection) => collection.status !== "rejected")
            .filter((collection) => !store.plannerDeselected[collection.collection]);
        const selected = selectedCollections.map((collection) => collection.collection);

        // Coming back to look at the plan and moving on again must not throw away what the
        // operator has since edited in the editor, so an unchanged plan is not applied twice.
        const appliedKey = computePlannerAppliedKey(conversationId, selectedCollections);
        if (appliedKey === store.plannerAppliedKey) {
            return;
        }

        const result = await api.services.migration.apply({
            slug: getValues("externalConnection").slug,
            conversationId,
            collections: selected,
        });

        if (result.errors.length > 0) {
            throw new WizardStepError("The registered mapping did not pass validation.", result.errors.join("\n"));
        }

        if (!result.configuration) {
            throw new WizardStepError("The planner returned no configuration to apply.");
        }

        // Tables the plan does not cover are not reported here: the editor's own UnmappedTablesAlert
        // derives the same list from the mapping it is about to render, and keeps deriving it as the
        // operator edits.
        adoptTables(parsePlannerTables(wrapDtoTablesToFormShape(result.configuration.tables ?? [])));
        store.setPlannerAppliedKey(appliedKey);
    };
}

/** Identifies what an apply would hand over: the session, and each kept collection at its version. */
export function computePlannerAppliedKey(conversationId: string, selected: PlannerCollection[]): string {
    const collections = selected
        .map((collection) => `${collection.collection}@${collection.version}`)
        .sort()
        .join(",");

    return `${conversationId}|${collections}`;
}
