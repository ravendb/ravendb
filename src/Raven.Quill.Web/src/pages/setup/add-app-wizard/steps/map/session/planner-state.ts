import type { NamingConventions, ValidationIssue } from "@/api/generated/server-api";
import type {
    PlannerChat,
    PlannerChatId,
    PlannerEdit,
    PlannerEntry,
    PlannerProposal,
    PlannerQuestion,
    PlannerReplyTurn,
    PlannerTurn,
    PlannerView,
    SelectedTables,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-types";

/**
 * The planner session. It lives in the wizard store rather than in the step body because the wizard
 * remounts the body on every step change, and because a turn keeps streaming after the operator leaves
 * the step. The server owns the plan and frames move it forward; plan/get reads it where frames fall short.
 */
export type PlannerState = {
    plannerSlug: string | null;
    /** The connect key the plan was made under; the server drops the plan on a connect to another source. */
    plannerConnectKey: string | null;
    plannerProposal: PlannerProposal | null;
    plannerConventions: NamingConventions | null;
    plannerEntries: Record<string, PlannerEntry>;
    /** The errors of the failed attempt, per collection the agent is retrying. */
    plannerRetrying: Record<string, string[]>;
    /** Kept here because the stored plan does not keep the issue paths of a rejection. */
    plannerRejectionIssues: Record<string, ValidationIssue[]>;
    /** Areas this session started. The plan does not record it. */
    plannerStartedAreas: string[];
    plannerProposalChat: PlannerChat;
    plannerAreaChats: Record<string, PlannerChat>;
    plannerSelectedTables: SelectedTables | null;
    /** Where a table without a schema is read from, as CDC Sink resolves it for the source's provider. */
    plannerDefaultSchema: string | null;
    plannerEdits: Record<string, PlannerEdit>;
    /** Goes up on every Discard, so the mapping editors start over from the plan. */
    plannerDiscardCount: number;
    /** Per collection, each path relative to the collection's mapping. */
    plannerSaveIssues: Record<string, ValidationIssue[]>;
    plannerSaveWarnings: string[];
    plannerIsSaving: boolean;
    plannerView: PlannerView;
    plannerIsChatOpen: boolean;
    plannerExpandedAreas: string[];
    plannerFilter: string;
};

export type PlannerChatPhase = "idle" | "working" | "awaitingAnswers" | "ready";

export const EMPTY_CHAT: PlannerChat = { turns: [], abortController: null, draft: "", mentions: [] };

export const PLAN_VIEW: PlannerView = { area: null, collection: null };

export const initialPlannerState: PlannerState = {
    plannerSlug: null,
    plannerConnectKey: null,
    plannerProposal: null,
    plannerConventions: null,
    plannerEntries: {},
    plannerRetrying: {},
    plannerRejectionIssues: {},
    plannerStartedAreas: [],
    plannerProposalChat: EMPTY_CHAT,
    plannerAreaChats: {},
    plannerSelectedTables: null,
    plannerDefaultSchema: null,
    plannerEdits: {},
    plannerDiscardCount: 0,
    plannerSaveIssues: {},
    plannerSaveWarnings: [],
    plannerIsSaving: false,
    plannerView: PLAN_VIEW,
    plannerIsChatOpen: true,
    plannerExpandedAreas: [],
    plannerFilter: "",
};

const NO_QUESTIONS: PlannerQuestion[] = [];

export function selectChat(
    state: Pick<PlannerState, "plannerProposalChat" | "plannerAreaChats">,
    chatId: PlannerChatId,
): PlannerChat {
    return chatId === null ? state.plannerProposalChat : (state.plannerAreaChats[chatId] ?? EMPTY_CHAT);
}

export function getLatestReply(turns: PlannerTurn[]): PlannerReplyTurn | undefined {
    return turns.findLast((turn) => turn.kind === "planner");
}

export function isChatWorking(chat: PlannerChat): boolean {
    return getLatestReply(chat.turns)?.status === "working";
}

export function getChatPhase(chat: PlannerChat): PlannerChatPhase {
    const latestReply = getLatestReply(chat.turns);

    if (!latestReply) {
        return "idle";
    }

    if (latestReply.status === "working") {
        return "working";
    }

    return getPendingQuestions(chat).length > 0 ? "awaitingAnswers" : "ready";
}

/** Questions can only be answered in a turn the agent finished; any other ending leaves nothing to answer. */
export function getPendingQuestions(chat: PlannerChat): PlannerQuestion[] {
    const latestTurn = chat.turns.at(-1);

    return latestTurn?.kind === "planner" && latestTurn.status === "done" ? latestTurn.questions : NO_QUESTIONS;
}

export function selectWorkingChats(state: PlannerState): PlannerChatId[] {
    const areas = Object.entries(state.plannerAreaChats)
        .filter(([, chat]) => isChatWorking(chat))
        .map(([area]) => area);

    return isChatWorking(state.plannerProposalChat) ? [null, ...areas] : areas;
}

export function selectIsAnyChatWorking(state: PlannerState): boolean {
    return selectWorkingChats(state).length > 0;
}

export function selectHasSession(state: PlannerState): boolean {
    return state.plannerProposal !== null || state.plannerProposalChat.turns.length > 0;
}

/**
 * The plan does not say which areas started, so an area also counts as started once something is mapped
 * there, as after a save moves a collection into it. The area/ask answer for an area that never started
 * corrects a wrong guess.
 */
export function isAreaStarted(
    state: Pick<PlannerState, "plannerStartedAreas" | "plannerEntries">,
    area: string,
): boolean {
    return (
        state.plannerStartedAreas.includes(area) ||
        Object.values(state.plannerEntries).some((entry) => entry.area === area && entry.status !== "Removed")
    );
}

export function selectAreConventionsLocked(state: PlannerState): boolean {
    return (state.plannerProposal?.areas ?? []).some((area) => !!area.area && isAreaStarted(state, area.area));
}

export function selectUnsavedCount(state: Pick<PlannerState, "plannerEdits">): number {
    return Object.keys(state.plannerEdits).length;
}

/** No chat may start a turn meanwhile: what the agents change would go stale under the operator's unsaved edits. */
export function selectHasUnsavedEdits(state: Pick<PlannerState, "plannerEdits">): boolean {
    return selectUnsavedCount(state) > 0;
}

export function selectCanEditPlan(state: PlannerState): boolean {
    return state.plannerProposal !== null && !state.plannerIsSaving && !selectIsAnyChatWorking(state);
}

export function updateChat(
    state: PlannerState,
    chatId: PlannerChatId,
    update: (chat: PlannerChat) => PlannerChat,
): Partial<PlannerState> {
    if (chatId === null) {
        return { plannerProposalChat: update(state.plannerProposalChat) };
    }

    return { plannerAreaChats: { ...state.plannerAreaChats, [chatId]: update(selectChat(state, chatId)) } };
}

export function omitKey<Value>(record: Record<string, Value>, key: string): Record<string, Value> {
    if (!(key in record)) {
        return record;
    }

    return Object.fromEntries(Object.entries(record).filter(([name]) => name !== key));
}
