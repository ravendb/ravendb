import type { WizardConfirmNext } from "@/components/form/wizard/form-wizard";
import { formatCount } from "@/lib/format";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { isEditorMapping } from "@/pages/setup/add-app-wizard/steps/map/map-source-options";
import { isBuilt } from "@/pages/setup/add-app-wizard/steps/map/session/planner-plan";
import { usePlan } from "@/pages/setup/add-app-wizard/steps/map/session/use-plan";

export function usePlannerUnbuiltConfirm(): WizardConfirmNext {
    const plan = usePlan();
    const isManualMapping = useSetupWizardStore((state) => isEditorMapping(state.mapOrigin));

    const unbuiltCollections = plan.collections.filter(
        (collection) => collection.state !== "removed" && !isBuilt(collection),
    );
    const unbuiltCount = unbuiltCollections.length;
    const unbuiltAreaCount = new Set(unbuiltCollections.map((collection) => collection.area)).size;

    return {
        isRequired: !isManualMapping && unbuiltCount > 0,
        title: "Create the app without all collections?",
        description:
            `${formatCount(unbuiltCount, "collection")} in ${formatCount(unbuiltAreaCount, "area")} ` +
            `${unbuiltCount === 1 ? "is" : "are"} not built and will be left out. Only built collections go to the app.`,
        confirmLabel: "Create app",
    };
}
