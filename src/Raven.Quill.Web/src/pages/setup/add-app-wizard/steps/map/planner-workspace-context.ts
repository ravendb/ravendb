import { createContext, useContext, type RefObject } from "react";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import {
    getChatPhase,
    isAreaStarted,
    selectCanEditPlan,
    selectChat,
    selectHasUnsavedEdits,
    type PlannerChatPhase,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-state";
import type { PlannerChatId } from "@/pages/setup/add-app-wizard/steps/map/session/planner-types";

type PlannerWorkspaceValue = {
    composerRef: RefObject<HTMLTextAreaElement | null>;
    /** The wizard is finishing the step; nothing in the planner may start a turn or change the plan meanwhile. */
    isBusy: boolean;
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

/** An area that never started has no agent yet, so its view shows the proposal agent's chat. */
export function useShownChatId(): PlannerChatId {
    return useSetupWizardStore((state) => {
        const area = state.plannerView.area;

        return area !== null && isAreaStarted(state, area) ? area : null;
    });
}

export function useChatPhase(chatId: PlannerChatId): PlannerChatPhase {
    return useSetupWizardStore((state) => getChatPhase(selectChat(state, chatId)));
}

export function useCanAsk(chatId: PlannerChatId): boolean {
    const phase = useChatPhase(chatId);
    const canSendMessage = useCanSendMessage(chatId);

    return phase !== "awaitingAnswers" && canSendMessage;
}

/** The operator may close the planner's questions and write in the chat instead; the message drops them. */
export function useCanSendMessage(chatId: PlannerChatId): boolean {
    const phase = useChatPhase(chatId);
    const isSaving = useSetupWizardStore((state) => state.plannerIsSaving);
    const hasUnsavedEdits = useSetupWizardStore(selectHasUnsavedEdits);
    const { isBusy } = usePlannerWorkspace();

    return phase !== "working" && !isSaving && !isBusy && !hasUnsavedEdits;
}

export function useCanEditPlan(): boolean {
    const canEdit = useSetupWizardStore(selectCanEditPlan);
    const { isBusy } = usePlannerWorkspace();

    return canEdit && !isBusy;
}
