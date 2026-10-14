import { createContext, useContext, type RefObject } from "react";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { selectPlannerPhase, type PlannerPhase } from "@/pages/setup/add-app-wizard/steps/map/session/planner-state";
import { useIsRemovingPlannerCollection } from "@/pages/setup/add-app-wizard/steps/map/session/use-remove-planner-collection";

type PlannerWorkspaceValue = {
    composerRef: RefObject<HTMLTextAreaElement | null>;
    /** The wizard is applying the plan; nothing in the planner may start a turn or change the plan meanwhile. */
    isBusy: boolean;
    isPanelOpen: boolean;
    /** No room for the plan beside the chat, so an open plan covers it. */
    isNarrow: boolean;
    changeSelection: () => void;
};

export const PlannerWorkspaceContext = createContext<PlannerWorkspaceValue | null>(null);

export function usePlannerWorkspace(): PlannerWorkspaceValue {
    const workspace = useContext(PlannerWorkspaceContext);

    if (workspace === null) {
        throw new Error("usePlannerWorkspace must be used inside PlannerWorkspace");
    }

    return workspace;
}

export function usePlannerChatWidthClass(): string {
    const { isPanelOpen, isNarrow } = usePlannerWorkspace();

    return isPanelOpen && !isNarrow ? "max-w-170" : "max-w-180";
}

export function useFocusPlannerComposer() {
    const { composerRef, isNarrow, isPanelOpen } = usePlannerWorkspace();
    const closePlannerPanel = useSetupWizardStore((state) => state.closePlannerPanel);

    return () => {
        if (isNarrow && isPanelOpen) {
            closePlannerPanel();
        }

        composerRef.current?.focus();
    };
}

/**
 * A prompt can go to the planner: it is ready for one, the wizard is not busy, and no removal is still
 * being saved - the turn would start from a plan that still holds the collection.
 */
export function useCanAskPlanner(): boolean {
    return useCanStartTurnWhen("ready");
}

export function useCanAnswerPlanner(): boolean {
    return useCanStartTurnWhen("awaitingAnswers");
}

function useCanStartTurnWhen(phase: PlannerPhase): boolean {
    const currentPhase = useSetupWizardStore(selectPlannerPhase);
    const { isBusy } = usePlannerWorkspace();
    const isRemoving = useIsRemovingPlannerCollection();

    return currentPhase === phase && !isBusy && !isRemoving;
}

/**
 * The composer takes what the operator writes - also while the planner works, for once it is done - but
 * not while the planner's questions stand in for it, nor once a failed opening left no conversation.
 */
export function useCanComposeForPlanner(): boolean {
    const phase = useSetupWizardStore(selectPlannerPhase);
    const { isBusy } = usePlannerWorkspace();

    return (phase === "opening" || phase === "working" || phase === "ready") && !isBusy;
}
