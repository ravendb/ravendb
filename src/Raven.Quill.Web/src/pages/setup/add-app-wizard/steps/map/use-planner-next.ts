import { useAiConsent } from "@/components/ai-consent/use-ai-consent";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { isEditorMapping } from "@/pages/setup/add-app-wizard/steps/map/map-source-options";
import { getPlannerNextState, type PlannerNextState } from "@/pages/setup/add-app-wizard/steps/map/planner-next";
import {
    getChatPhase,
    selectHasSession,
    selectIsAnyChatWorking,
    selectUnsavedCount,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-state";
import { countBuilt, isBuilt } from "@/pages/setup/add-app-wizard/steps/map/session/planner-plan";
import { usePlan } from "@/pages/setup/add-app-wizard/steps/map/session/use-plan";

export function usePlannerNext(): PlannerNextState {
    const consent = useAiConsent();
    const plan = usePlan();
    const isManualMapping = useSetupWizardStore((state) => isEditorMapping(state.mapOrigin));
    const hasSession = useSetupWizardStore(selectHasSession);
    const isWorking = useSetupWizardStore(selectIsAnyChatWorking);
    const isAwaitingAnswers = useSetupWizardStore(
        (state) => getChatPhase(state.plannerProposalChat) === "awaitingAnswers",
    );
    const isSaving = useSetupWizardStore((state) => state.plannerIsSaving);
    const unsavedCount = useSetupWizardStore(selectUnsavedCount);
    const collections = plan.collections.filter((collection) => collection.state !== "removed");

    return getPlannerNextState({
        consent,
        isManualMapping,
        hasSession,
        isWorking,
        isAwaitingAnswers,
        isSaving,
        unsavedCount,
        builtCount: countBuilt(collections),
        unbuiltCount: collections.filter((collection) => !isBuilt(collection)).length,
    });
}
