import type { StateCreator } from "zustand";
import type { MigrationFrame } from "@/api/generated/server-api";
import type { SetupWizardState } from "@/pages/setup/add-app-wizard/app-wizard-store";
import {
    applyPlannerFrame,
    createReplyTurn,
    endReplyTurn,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-frames";
import { getPlanRevisions } from "@/pages/setup/add-app-wizard/steps/map/session/planner-plan";
import {
    initialPlannerState,
    withoutCollection,
    withoutRejections,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-state";
import type {
    PlannerAnswersTurn,
    PlannerPromptTurn,
    SelectedTables,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-types";

export type PlannerActions = {
    openPlannerSession: (session: { selectedTables: SelectedTables; controller: AbortController }) => void;
    beginPlannerTurn: (turn: { userTurn: PlannerPromptTurn | PlannerAnswersTurn; controller: AbortController }) => void;
    receivePlannerFrame: (frame: MigrationFrame) => void;
    endPlannerTurn: (ending: { error: string | null; isStopped: boolean }) => void;
    stopPlannerTurn: () => void;
    removePlannerCollection: (collection: string) => void;
    dismissPlannerRejections: (collections: string[]) => void;
    setPlannerDraft: (draft: string) => void;
    addPlannerMention: (collection: string) => void;
    removePlannerMention: (collection: string) => void;
    clearPlannerComposer: () => void;
    openPlannerPanel: (detailsCollection?: string | null) => void;
    closePlannerPanel: () => void;
    /** Clears the session for a new one, keeping what the operator wrote so it goes to the new planner. */
    startPlannerOver: () => void;
    resetPlannerState: () => void;
};

export const createPlannerSlice: StateCreator<SetupWizardState, [], [], PlannerActions> = (set, get) => ({
    openPlannerSession: ({ selectedTables, controller }) =>
        set({
            plannerTurns: [createReplyTurn(true)],
            plannerSelectedTables: selectedTables,
            plannerAbortController: controller,
        }),

    beginPlannerTurn: ({ userTurn, controller }) => {
        // The previous turn is finished, but its stream may still be closing; nothing it sends now counts.
        get().plannerAbortController?.abort();
        set((state) => ({
            plannerTurns: [...state.plannerTurns, userTurn, createReplyTurn(false)],
            plannerAbortController: controller,
        }));
    },

    receivePlannerFrame: (frame) => set((state) => applyPlannerFrame(state, frame)),

    endPlannerTurn: (ending) =>
        set((state) => ({ plannerTurns: endReplyTurn(state.plannerTurns, ending), plannerAbortController: null })),

    stopPlannerTurn: () => get().plannerAbortController?.abort(),

    removePlannerCollection: (collection) => set((state) => withoutCollection(state, collection)),

    dismissPlannerRejections: (collections) => set((state) => withoutRejections(state, collections)),

    setPlannerDraft: (draft) => set({ plannerDraft: draft }),

    addPlannerMention: (collection) =>
        set((state) => ({
            plannerMentions: state.plannerMentions.includes(collection)
                ? state.plannerMentions
                : [...state.plannerMentions, collection],
        })),

    removePlannerMention: (collection) =>
        set((state) => ({ plannerMentions: state.plannerMentions.filter((name) => name !== collection) })),

    clearPlannerComposer: () => set({ plannerDraft: "", plannerMentions: [] }),

    openPlannerPanel: (detailsCollection = null) =>
        set({ plannerPanel: "open", plannerDetailsCollection: detailsCollection, plannerSeenRevisions: {} }),

    closePlannerPanel: () =>
        set((state) => ({
            plannerPanel: "closed",
            plannerDetailsCollection: null,
            plannerSeenRevisions: getPlanRevisions(state),
        })),

    startPlannerOver: () => {
        get().plannerAbortController?.abort();
        set((state) => ({ ...initialPlannerState, plannerDraft: state.plannerDraft }));
    },

    resetPlannerState: () => {
        get().plannerAbortController?.abort();
        set(initialPlannerState);
    },
});
