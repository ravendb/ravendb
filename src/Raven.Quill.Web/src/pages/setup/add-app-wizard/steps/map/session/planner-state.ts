import type {
    PlannerCollection,
    PlannerProposal,
    PlannerQuestion,
    PlannerReplyTurn,
    PlannerTurn,
    RegisteredCollection,
    SelectedTables,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-types";

/**
 * The planner session. It lives in the wizard store rather than in the step body because the wizard
 * remounts the body on every step change - holding the conversation in component state would mean
 * leaving the step and coming back could only start over, never resume.
 */
export type PlannerState = {
    plannerTurns: PlannerTurn[];
    plannerConversationId: string | null;
    plannerProposal: PlannerProposal | null;
    plannerCollections: Record<string, PlannerCollection>;
    plannerRegistrationCount: number;
    /** Collections taken out of the plan since the proposal, which still lists them. */
    plannerRemovedCollections: string[];
    plannerSelectedTables: SelectedTables | null;
    /** Aborts the stream of the turn in flight. Whether a turn is running is the turn's own status. */
    plannerAbortController: AbortController | null;
    /** What the operator is writing. Written while a turn runs, it waits for the planner to finish. */
    plannerDraft: string;
    plannerMentions: string[];
    /** "auto" opens the plan when there is room for it beside the chat; the operator's choice wins. */
    plannerPanel: "auto" | "open" | "closed";
    plannerDetailsCollection: string | null;
    /** Each collection's revision when the panel was last closed, so the Plan button can flag changes. */
    plannerSeenRevisions: Record<string, string>;
};

export type PlannerPhase = "idle" | "opening" | "working" | "awaitingAnswers" | "ready" | "failed";

export const initialPlannerState: PlannerState = {
    plannerTurns: [],
    plannerConversationId: null,
    plannerProposal: null,
    plannerCollections: {},
    plannerRegistrationCount: 0,
    plannerRemovedCollections: [],
    plannerSelectedTables: null,
    plannerAbortController: null,
    plannerDraft: "",
    plannerMentions: [],
    plannerPanel: "auto",
    plannerDetailsCollection: null,
    plannerSeenRevisions: {},
};

const NO_QUESTIONS: PlannerQuestion[] = [];

export function getLatestReply(turns: PlannerTurn[]): PlannerReplyTurn | undefined {
    return turns.findLast((turn) => turn.kind === "planner");
}

/**
 * Where the session stands, from its latest reply: every surface reads it from here, so the composer,
 * the plan and the footer never disagree about whether the planner is busy or waiting on the operator.
 */
export function selectPlannerPhase(state: PlannerState): PlannerPhase {
    const latestReply = getLatestReply(state.plannerTurns);

    if (!latestReply) {
        return "idle";
    }

    if (latestReply.status === "working") {
        return latestReply.isOpening ? "opening" : "working";
    }

    if (state.plannerConversationId === null) {
        return "failed";
    }

    return selectPendingQuestions(state).length > 0 ? "awaitingAnswers" : "ready";
}

/** The planner is not working and the session is open, so the plan holds still: it can be applied or changed by hand. */
export function selectIsPlanSettled(state: PlannerState): boolean {
    const phase = selectPlannerPhase(state);

    return phase === "ready" || phase === "awaitingAnswers";
}

/** Questions can only be answered in a turn the planner finished; any other ending leaves nothing to answer. */
export function selectPendingQuestions(state: PlannerState): PlannerQuestion[] {
    const latestTurn = state.plannerTurns.at(-1);

    return latestTurn?.kind === "planner" && latestTurn.status === "done" ? latestTurn.questions : NO_QUESTIONS;
}

export function isRegistered(collection: PlannerCollection): collection is RegisteredCollection {
    return collection.outcome === "registered";
}

export function selectBuiltCollections(state: Pick<PlannerState, "plannerCollections">): RegisteredCollection[] {
    return Object.values(state.plannerCollections).filter(isRegistered);
}

/** Lets go of the rejected changes the operator chose not to retry, leaving each collection as it is built. */
export function withoutRejections(state: PlannerState, collections: string[]): Partial<PlannerState> {
    return {
        plannerCollections: Object.fromEntries(
            Object.entries(state.plannerCollections).map(([name, collection]) => [
                name,
                collections.includes(name) && isRegistered(collection)
                    ? { ...collection, rejectionErrors: [] }
                    : collection,
            ]),
        ),
    };
}

/** Takes a collection out of every part of the session that refers to it, however it was removed. */
export function withoutCollection(state: PlannerState, collection: string): Partial<PlannerState> {
    return {
        plannerCollections: omitKey(state.plannerCollections, collection),
        plannerRemovedCollections: state.plannerRemovedCollections.includes(collection)
            ? state.plannerRemovedCollections
            : [...state.plannerRemovedCollections, collection],
        plannerMentions: state.plannerMentions.filter((name) => name !== collection),
        plannerDetailsCollection: state.plannerDetailsCollection === collection ? null : state.plannerDetailsCollection,
    };
}

function omitKey<Value>(record: Record<string, Value>, key: string): Record<string, Value> {
    return Object.fromEntries(Object.entries(record).filter(([name]) => name !== key));
}
