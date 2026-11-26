import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { patchEdit } from "@/pages/setup/add-app-wizard/steps/map/session/planner-edits";
import type { PlanCollection } from "@/pages/setup/add-app-wizard/steps/map/session/planner-plan";

export function useToggleRemoval(collection: PlanCollection) {
    const stagePlannerEdit = useSetupWizardStore((state) => state.stagePlannerEdit);

    return () =>
        stagePlannerEdit(
            collection.name,
            patchEdit(collection.area, collection.edit, { isRemoved: !collection.edit?.isRemoved }),
        );
}
