import type { MigrationFrame, MigrationFrameConventionsFrame, OpenQuestion } from "@/api/generated/server-api";
import {
    isRegistered,
    withoutCollection,
    type PlannerState,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-state";
import {
    resolveConfigTables,
    resolveProposalTables,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-table-refs";
import type {
    PlannerActivity,
    PlannerQuestion,
    PlannerReplyTurn,
    PlannerTurn,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-types";

export const UNSPECIFIED_PROPERTY_CASE = "Unspecified";

const INCOMPLETE_TURN_ERROR = "The planner ended its turn without finishing it.";

let nextTurnId = 0;

export function createTurnId(): string {
    nextTurnId += 1;
    return `planner-turn-${nextTurnId}`;
}

export function createReplyTurn(isOpening: boolean): PlannerReplyTurn {
    return {
        kind: "planner",
        id: createTurnId(),
        isOpening,
        status: "working",
        activity: [],
        reply: null,
        gaps: [],
        questions: [],
        error: null,
    };
}

/**
 * Folds one streamed frame into the session. Every frame belongs to the reply turn the stream opened,
 * which is always the last one: the transcript only grows at its end.
 */
export function applyPlannerFrame(state: PlannerState, frame: MigrationFrame): Partial<PlannerState> {
    const selectedTables = state.plannerSelectedTables ?? [];

    switch (frame.type) {
        case "proposal":
            return {
                plannerProposal: resolveProposalTables(frame, selectedTables),
                plannerRemovedCollections: [],
                plannerTurns: addActivity(state.plannerTurns, {
                    type: "proposal",
                    collections: frame.collections.flatMap((proposed) =>
                        proposed.collection ? [proposed.collection] : [],
                    ),
                    areaCount: frame.areas.length,
                    droppedCount: frame.dropped.length,
                }),
            };

        case "collection": {
            const previous = state.plannerCollections[frame.collection];
            const registrationNumber = state.plannerRegistrationCount + 1;

            return {
                plannerCollections: {
                    ...state.plannerCollections,
                    [frame.collection]: {
                        name: frame.collection,
                        outcome: "registered",
                        version: frame.version,
                        registrationNumber,
                        rationale: frame.rationale ?? null,
                        config: frame.config ?? null,
                        tableRef: frame.config ? resolveConfigTables(frame.config, selectedTables) : null,
                        previousConfig: previous && isRegistered(previous) ? previous.config : null,
                        warnings: frame.warnings,
                        rejectionErrors: [],
                    },
                },
                plannerRegistrationCount: registrationNumber,
                plannerRemovedCollections: state.plannerRemovedCollections.filter((name) => name !== frame.collection),
                plannerTurns: addActivity(state.plannerTurns, {
                    type: "registered",
                    collection: frame.collection,
                    version: frame.version,
                    warningCount: frame.warnings.length,
                }),
            };
        }

        // Kept as a collection rather than dropped: a rejection the operator cannot see looks identical
        // to a collection the planner never attempted. A rejected change leaves the registration in place,
        // as it does on the server.
        case "rejected": {
            const previous = state.plannerCollections[frame.collection];
            const isChange = previous !== undefined && isRegistered(previous);

            return {
                plannerCollections: isChange
                    ? {
                          ...state.plannerCollections,
                          [frame.collection]: { ...previous, rejectionErrors: frame.errors },
                      }
                    : {
                          ...state.plannerCollections,
                          [frame.collection]: { name: frame.collection, outcome: "rejected", errors: frame.errors },
                      },
                plannerTurns: addActivity(state.plannerTurns, {
                    type: "rejected",
                    collection: frame.collection,
                    errorCount: frame.errors.length,
                    isChange,
                }),
            };
        }

        case "removed":
            return frame.collection
                ? {
                      ...withoutCollection(state, frame.collection),
                      plannerTurns: addActivity(state.plannerTurns, { type: "removed", collection: frame.collection }),
                  }
                : {};

        case "conventions":
            return isUnchangedConventions(frame)
                ? {}
                : {
                      plannerTurns: addActivity(state.plannerTurns, {
                          type: "conventions",
                          propertyCase: frame.propertyCase,
                          propertyLanguage: frame.propertyLanguage ?? null,
                          notes: frame.notes ?? null,
                          mustReEmit: frame.mustReEmit,
                      }),
                  };

        case "note":
            return { plannerTurns: addActivity(state.plannerTurns, { type: "note", text: frame.text }) };

        case "reply":
            return {
                plannerTurns: updateReplyTurn(state.plannerTurns, (turn) => ({
                    ...turn,
                    reply: frame.reply?.trim() || null,
                    gaps: frame.gaps,
                    questions: frame.openQuestions.map(toPlannerQuestion),
                })),
            };

        case "done":
            return {
                plannerConversationId: frame.conversationId,
                plannerTurns: updateReplyTurn(state.plannerTurns, (turn) => ({ ...turn, status: "done" })),
            };

        case "error":
            return {
                plannerTurns: updateReplyTurn(state.plannerTurns, (turn) => failTurn(turn, frame.message)),
            };
    }
}

/**
 * Settles the reply turn once its stream ends. A stream that ends without the planner saying it is done
 * left the turn unfinished, and the operator stopping it is not a failure.
 */
export function endReplyTurn(turns: PlannerTurn[], { error, isStopped }: { error: string | null; isStopped: boolean }) {
    return updateReplyTurn(turns, (turn): PlannerReplyTurn => {
        if (error !== null) {
            return failTurn(turn, error);
        }

        if (turn.status !== "working") {
            return turn;
        }

        return isStopped ? { ...turn, status: "stopped" } : { ...turn, status: "failed", error: INCOMPLETE_TURN_ERROR };
    });
}

/** A turn the planner already finished keeps its outcome: the conversation and the plan are saved by then. */
function failTurn(turn: PlannerReplyTurn, error: string): PlannerReplyTurn {
    return turn.status === "done"
        ? { ...turn, activity: [...turn.activity, { type: "lateError", message: error }] }
        : { ...turn, status: "failed", error };
}

function isUnchangedConventions(frame: MigrationFrameConventionsFrame): boolean {
    return (
        frame.propertyCase === UNSPECIFIED_PROPERTY_CASE &&
        !frame.propertyLanguage &&
        !frame.notes &&
        frame.mustReEmit.length === 0
    );
}

function toPlannerQuestion({ question, options, recommended }: OpenQuestion): PlannerQuestion {
    const isRecommendationValid = recommended != null && recommended >= 0 && recommended < options.length;

    return { question, options, recommended: isRecommendationValid ? recommended : null };
}

function addActivity(turns: PlannerTurn[], activity: PlannerActivity): PlannerTurn[] {
    return updateReplyTurn(turns, (turn) => ({ ...turn, activity: [...turn.activity, activity] }));
}

function updateReplyTurn(turns: PlannerTurn[], update: (turn: PlannerReplyTurn) => PlannerReplyTurn): PlannerTurn[] {
    const last = turns.at(-1);

    if (last?.kind !== "planner") {
        return turns;
    }

    return [...turns.slice(0, -1), update(last)];
}
