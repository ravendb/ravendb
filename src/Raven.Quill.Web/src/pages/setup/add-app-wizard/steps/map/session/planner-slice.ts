import type { StateCreator } from "zustand";
import { api } from "@/api/api";
import type { MigrationFrame, MigrationPlanGetResponse } from "@/api/generated/server-api";
import type { SetupWizardState } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { isEditorMapping, type MapOrigin } from "@/pages/setup/add-app-wizard/steps/map/map-source-options";
import {
    applyPlannerFrame,
    applyStoredPlan,
    createReplyTurn,
    endPlannerTurn,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-frames";
import {
    initialPlannerState,
    omitKey,
    selectChat,
    selectWorkingChats,
    updateChat,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-state";
import type {
    PlannerAnswersTurn,
    PlannerChatId,
    PlannerEdit,
    PlannerPromptTurn,
    PlannerTurnEnding,
    PlannerView,
    SelectedTables,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-types";

export type PlannerSessionInput = {
    slug: string;
    connectKey: string;
    selectedTables: SelectedTables;
    defaultSchema: string;
};

/** A session the server already holds the plan of, opened again as it stands. */
export type RestoredPlannerSession = { session: PlannerSessionInput; plan: MigrationPlanGetResponse };

export type PlannerActions = {
    openPlannerSession: (session: PlannerSessionInput) => void;
    restorePlannerSession: (restored: RestoredPlannerSession) => void;
    beginPlannerTurn: (turn: {
        chatId: PlannerChatId;
        userTurn: PlannerPromptTurn | PlannerAnswersTurn | null;
        controller: AbortController;
        isOpening: boolean;
    }) => void;
    receivePlannerFrame: (chatId: PlannerChatId, frame: MigrationFrame) => void;
    endPlannerTurn: (chatId: PlannerChatId, ending: PlannerTurnEnding) => void;
    receivePlannerPlan: (response: MigrationPlanGetResponse) => void;
    markPlannerAreaStarted: (area: string, isStarted: boolean) => void;
    /** Ends a chat's running turn on the server. Its stream closes by itself once the turn ends. */
    stopPlannerTurn: (chatId: PlannerChatId) => Promise<void>;
    /** Ends every running turn on the server too, not only its stream here. */
    stopPlannerTurns: () => Promise<void>;
    setPlannerDraft: (chatId: PlannerChatId, draft: string) => void;
    addPlannerMention: (chatId: PlannerChatId, collection: string) => void;
    removePlannerMention: (chatId: PlannerChatId, collection: string) => void;
    clearPlannerComposer: (chatId: PlannerChatId) => void;
    stagePlannerEdit: (collection: string, edit: PlannerEdit | null) => void;
    discardPlannerEdits: () => void;
    dismissPlannerSaveWarnings: () => void;
    setPlannerSaving: (isSaving: boolean) => void;
    receivePlannerSave: (outcome: {
        issues: SetupWizardState["plannerSaveIssues"];
        warnings: string[];
        isSaved: boolean;
    }) => void;
    showPlannerView: (view: PlannerView) => void;
    setPlannerChatOpen: (isOpen: boolean) => void;
    togglePlannerArea: (area: string) => void;
    setPlannerExpandedAreas: (areas: string[]) => void;
    setPlannerFilter: (filter: string) => void;
    /** Discards the plan on the server too, so a reloaded wizard does not bring it back. */
    startPlannerOver: () => void;
    resetPlannerState: () => void;
};

export const createPlannerSlice: StateCreator<SetupWizardState, [], [], PlannerActions> = (set, get) => {
    const stopTurns = async () => {
        const state = get();
        const workingChats = selectWorkingChats(state);

        for (const chatId of workingChats) {
            selectChat(state, chatId).abortController?.abort();
        }

        await Promise.allSettled(workingChats.map((chatId) => stopTurnOnServer(state.plannerSlug, chatId)));
    };

    const abortStreams = () => {
        const state = get();

        state.plannerProposalChat.abortController?.abort();
        Object.values(state.plannerAreaChats).forEach((chat) => chat.abortController?.abort());
    };

    return {
        openPlannerSession: ({ slug, connectKey, selectedTables, defaultSchema }) =>
            set({
                plannerSlug: slug,
                plannerConnectKey: connectKey,
                plannerSelectedTables: selectedTables,
                plannerDefaultSchema: defaultSchema,
            }),

        restorePlannerSession: ({ session, plan }) => {
            get().openPlannerSession(session);
            get().receivePlannerPlan(plan);
        },

        beginPlannerTurn: ({ chatId, userTurn, controller, isOpening }) => {
            // The chat's previous turn is finished, but its stream may still be closing; nothing it sends now counts.
            selectChat(get(), chatId).abortController?.abort();
            set((state) =>
                updateChat(state, chatId, (chat) => ({
                    ...chat,
                    turns: [...chat.turns, ...(userTurn ? [userTurn] : []), createReplyTurn(isOpening)],
                    abortController: controller,
                })),
            );
        },

        receivePlannerFrame: (chatId, frame) => set((state) => applyPlannerFrame(state, chatId, frame)),

        endPlannerTurn: (chatId, ending) => set((state) => endPlannerTurn(state, chatId, ending)),

        receivePlannerPlan: (response) => set((state) => applyStoredPlan(state, response)),

        markPlannerAreaStarted: (area, isStarted) =>
            set((state) => ({
                plannerStartedAreas: isStarted
                    ? [...new Set([...state.plannerStartedAreas, area])]
                    : state.plannerStartedAreas.filter((name) => name !== area),
            })),

        stopPlannerTurn: (chatId) => stopTurnOnServer(get().plannerSlug, chatId),

        stopPlannerTurns: stopTurns,

        setPlannerDraft: (chatId, draft) => set((state) => updateChat(state, chatId, (chat) => ({ ...chat, draft }))),

        addPlannerMention: (chatId, collection) =>
            set((state) =>
                updateChat(state, chatId, (chat) => ({
                    ...chat,
                    mentions: chat.mentions.includes(collection) ? chat.mentions : [...chat.mentions, collection],
                })),
            ),

        removePlannerMention: (chatId, collection) =>
            set((state) =>
                updateChat(state, chatId, (chat) => ({
                    ...chat,
                    mentions: chat.mentions.filter((name) => name !== collection),
                })),
            ),

        clearPlannerComposer: (chatId) =>
            set((state) => updateChat(state, chatId, (chat) => ({ ...chat, draft: "", mentions: [] }))),

        stagePlannerEdit: (collection, edit) =>
            set((state) => ({
                plannerEdits: edit
                    ? { ...state.plannerEdits, [collection]: edit }
                    : omitKey(state.plannerEdits, collection),
                plannerSaveIssues: omitKey(state.plannerSaveIssues, collection),
            })),

        discardPlannerEdits: () =>
            set((state) => ({
                plannerEdits: {},
                plannerDiscardCount: state.plannerDiscardCount + 1,
                plannerSaveIssues: {},
                plannerSaveWarnings: [],
            })),

        dismissPlannerSaveWarnings: () => set({ plannerSaveWarnings: [] }),

        setPlannerSaving: (isSaving) => set({ plannerIsSaving: isSaving }),

        receivePlannerSave: ({ issues, warnings, isSaved }) =>
            set(
                isSaved
                    ? { plannerEdits: {}, plannerSaveIssues: {}, plannerSaveWarnings: warnings }
                    : { plannerSaveIssues: issues, plannerSaveWarnings: warnings },
            ),

        showPlannerView: (view) => set({ plannerView: view }),

        setPlannerChatOpen: (isOpen) => set({ plannerIsChatOpen: isOpen }),

        togglePlannerArea: (area) =>
            set((state) => ({
                plannerExpandedAreas: state.plannerExpandedAreas.includes(area)
                    ? state.plannerExpandedAreas.filter((name) => name !== area)
                    : [...state.plannerExpandedAreas, area],
            })),

        setPlannerExpandedAreas: (areas) => set({ plannerExpandedAreas: areas }),

        setPlannerFilter: (filter) => set({ plannerFilter: filter }),

        startPlannerOver: () => {
            const slug = get().plannerSlug;

            // A turn that is still ending could store its plan after the discard.
            void stopTurns().then(() => discardPlanOnServer(slug));
            set((state) => ({
                ...initialPlannerState,
                plannerSlug: state.plannerSlug,
                plannerIsChatOpen: state.plannerIsChatOpen,
                mapOrigin: withoutAppliedPlan(state.mapOrigin),
            }));
        },

        resetPlannerState: () => {
            void stopTurns();
            abortStreams();
            set((state) => ({ ...initialPlannerState, mapOrigin: withoutAppliedPlan(state.mapOrigin) }));
        },
    };
};

/** The next plan can have the same collections and versions, so it must not pass for the applied one. */
function withoutAppliedPlan(origin: MapOrigin | null): MapOrigin | null {
    return isEditorMapping(origin) ? origin : null;
}

// A failed discard only costs a reload its fresh start, so it does not hold the wizard back.
async function discardPlanOnServer(slug: string | null): Promise<void> {
    if (!slug) {
        return;
    }

    try {
        await api.services.setup.migrationPlanDiscard({ slug });
    } catch (error) {
        console.error("Could not discard the plan:", error);
    }
}

function stopTurnOnServer(slug: string | null, chatId: PlannerChatId): Promise<void> {
    if (!slug) {
        return Promise.resolve();
    }

    return api.services.setup.migrationStop(
        chatId === null ? { slug, agent: "Proposal" } : { slug, agent: "Area", area: chatId },
    );
}
