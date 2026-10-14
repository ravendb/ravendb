import { useFormContext, useWatch } from "react-hook-form";
import { useAiConsent } from "@/components/ai-consent/use-ai-consent";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { getPlannerNextState, type PlannerNextState } from "@/pages/setup/add-app-wizard/steps/map/planner-next";
import { countUnbuilt } from "@/pages/setup/add-app-wizard/steps/map/planner-plan";
import { selectBuiltCollections, selectPlannerPhase } from "@/pages/setup/add-app-wizard/steps/map/planner-state";
import { computePlannerOriginKey } from "@/pages/setup/add-app-wizard/steps/map/use-apply-planner";
import { useIsRemovingPlannerCollection } from "@/pages/setup/add-app-wizard/steps/map/use-remove-planner-collection";

/** Read by the wizard's step list, so it selects the plan's parts and derives from them only when they change. */
export function usePlannerNext(): PlannerNextState {
    const { control } = useFormContext<AppFormData>();
    const consent = useAiConsent();
    const hasMapping = useWatch({ control, name: "mapTables.tables", compute: (tables) => tables.length > 0 });
    const isRemoving = useIsRemovingPlannerCollection();
    const phase = useSetupWizardStore(selectPlannerPhase);
    const plannerProposal = useSetupWizardStore((state) => state.plannerProposal);
    const plannerCollections = useSetupWizardStore((state) => state.plannerCollections);
    const plannerRemovedCollections = useSetupWizardStore((state) => state.plannerRemovedCollections);
    const conversationId = useSetupWizardStore((state) => state.plannerConversationId);
    const mapOriginKey = useSetupWizardStore((state) => state.mapOrigin?.key);

    const built = selectBuiltCollections({ plannerCollections });
    const isMappingFromPlan =
        hasMapping && conversationId !== null && mapOriginKey === computePlannerOriginKey(conversationId, built);

    return getPlannerNextState({
        phase,
        consent,
        builtCount: built.length,
        unbuiltCount: countUnbuilt({ plannerProposal, plannerCollections, plannerRemovedCollections }),
        hasMapping,
        isMappingFromPlan,
        isRemoving,
    });
}
