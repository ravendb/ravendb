import { useFormContext, useWatch } from "react-hook-form";
import { useAiConsent } from "@/components/ai-consent/use-ai-consent";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { getPlannerNextState, type PlannerNextState } from "@/pages/setup/add-app-wizard/steps/map/planner-next";
import { countUnbuilt } from "@/pages/setup/add-app-wizard/steps/map/session/planner-plan";
import {
    selectBuiltCollections,
    selectPlannerPhase,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-state";
import { useIsRemovingPlannerCollection } from "@/pages/setup/add-app-wizard/steps/map/session/use-remove-planner-collection";
import { computePlanKey, isPlanApplied } from "@/pages/setup/add-app-wizard/steps/map/use-apply-planner";

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
    const mapOrigin = useSetupWizardStore((state) => state.mapOrigin);

    const built = selectBuiltCollections({ plannerCollections });
    const isMappingFromPlan =
        hasMapping && conversationId !== null && isPlanApplied(mapOrigin, computePlanKey(conversationId, built));

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
