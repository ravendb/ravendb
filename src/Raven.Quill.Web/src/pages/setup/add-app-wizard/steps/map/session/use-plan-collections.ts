import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { indexDiscovery } from "@/pages/setup/add-app-wizard/steps/map/session/planner-document";
import { buildPlanCollections, type PlanCollection } from "@/pages/setup/add-app-wizard/steps/map/session/planner-plan";

export function usePlanCollections(): PlanCollection[] {
    const plannerProposal = useSetupWizardStore((state) => state.plannerProposal);
    const plannerCollections = useSetupWizardStore((state) => state.plannerCollections);
    const plannerRemovedCollections = useSetupWizardStore((state) => state.plannerRemovedCollections);
    const discoverResult = useSetupWizardStore((state) => state.discoverResult);

    return buildPlanCollections(
        { plannerProposal, plannerCollections, plannerRemovedCollections },
        indexDiscovery(discoverResult),
    );
}
