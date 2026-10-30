import type {
    CdcSinkTableConfig,
    MigrationFrame,
    MigrationPlanGetResponse,
    OpenQuestion,
} from "@/api/generated/server-api";
import {
    getLatestReply,
    omitKey,
    selectChat,
    selectWorkingChats,
    updateChat,
    type PlannerState,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-state";
import {
    resolveConfigTables,
    resolveProposalTables,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-table-refs";
import type {
    PlannerActivity,
    PlannerChatId,
    PlannerEntry,
    PlannerQuestion,
    PlannerReplyTurn,
    PlannerTurn,
    PlannerTurnEnding,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-types";

export const UNSPECIFIED_PROPERTY_CASE = "Unspecified";

/** Quill ends a stream with this error frame after migrationStop. */
export const STOPPED_TURN_ERROR = "The turn was stopped.";

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

export function isAreaNotStartedError(message: string): boolean {
    return /^The area .+ has not started; start it first\.$/.test(message.trim());
}

/**
 * A frame belongs to its chat's last turn, the reply its stream opened. What an area agent registers is in
 * that area.
 */
export function applyPlannerFrame(
    state: PlannerState,
    chatId: PlannerChatId,
    frame: MigrationFrame,
): Partial<PlannerState> {
    const addActivity = (activity: PlannerActivity) =>
        updateChat(state, chatId, (chat) => ({
            ...chat,
            turns: updateReplyTurn(chat.turns, (turn) => ({ ...turn, activity: [...turn.activity, activity] })),
        }));
    const resolveTables = (config: CdcSinkTableConfig) =>
        resolveConfigTables(config, state.plannerSelectedTables ?? [], state.plannerDefaultSchema);

    switch (frame.type) {
        case "proposal":
            return {
                plannerProposal: resolveProposalTables(frame.proposal, state.plannerSelectedTables ?? []),
                ...addActivity({ type: "proposal" }),
            };

        case "conventions":
            return {
                plannerConventions: {
                    propertyCase: frame.propertyCase,
                    propertyLanguage: frame.propertyLanguage ?? null,
                    notes: frame.notes ?? null,
                },
                ...addActivity({
                    type: "conventions",
                    propertyCase: frame.propertyCase,
                    propertyLanguage: frame.propertyLanguage ?? null,
                    notes: frame.notes ?? null,
                }),
            };

        case "collection": {
            const previous = state.plannerEntries[frame.collection];
            const entry: PlannerEntry = {
                name: frame.collection,
                area: chatId ?? previous?.area ?? null,
                status: "Registered",
                version: frame.version,
                rationale: frame.rationale ?? previous?.rationale ?? null,
                config: frame.config ?? previous?.config ?? null,
                tableRef: frame.config ? resolveTables(frame.config) : (previous?.tableRef ?? null),
                warnings: frame.warnings,
            };

            return {
                plannerEntries: { ...state.plannerEntries, [frame.collection]: entry },
                plannerRetrying: omitKey(state.plannerRetrying, frame.collection),
                plannerRejectionIssues: omitKey(state.plannerRejectionIssues, frame.collection),
                ...addActivity({
                    type: "registered",
                    collection: frame.collection,
                    version: frame.version,
                    warningCount: frame.warnings.length,
                }),
            };
        }

        case "transientCollectionError":
            return {
                plannerRetrying: {
                    ...state.plannerRetrying,
                    [frame.collection]: frame.errors.map((issue) => issue.message),
                },
                ...addActivity({
                    type: "transientError",
                    collection: frame.collection,
                    errors: frame.errors.map((issue) => issue.message),
                }),
            };

        // A rejected change leaves a registered collection as it was, as the plan does; a collection that
        // never registered is kept with the agent's last attempt, to be fixed by hand or retried.
        case "rejectedCollection": {
            const previous = state.plannerEntries[frame.collection];
            const isChange = previous?.status === "Registered";
            const config = frame.config ?? previous?.config ?? null;

            return {
                plannerEntries: isChange
                    ? state.plannerEntries
                    : {
                          ...state.plannerEntries,
                          [frame.collection]: {
                              name: frame.collection,
                              area: chatId ?? previous?.area ?? null,
                              status: "Rejected",
                              version: previous?.version ?? 0,
                              rationale: previous?.rationale ?? null,
                              config,
                              tableRef: config ? resolveTables(config) : null,
                              warnings: frame.errors.map((issue) => issue.message),
                          },
                      },
                plannerRetrying: omitKey(state.plannerRetrying, frame.collection),
                plannerRejectionIssues: { ...state.plannerRejectionIssues, [frame.collection]: frame.errors },
                ...addActivity({
                    type: "rejected",
                    collection: frame.collection,
                    errorCount: frame.errors.length,
                    isChange,
                }),
            };
        }

        case "removed": {
            const collection = frame.collection;
            const previous = collection ? state.plannerEntries[collection] : undefined;

            if (!collection) {
                return {};
            }

            return {
                plannerEntries: previous
                    ? { ...state.plannerEntries, [collection]: { ...previous, status: "Removed" } }
                    : state.plannerEntries,
                plannerRetrying: omitKey(state.plannerRetrying, collection),
                ...addActivity({ type: "removed", collection, reason: frame.reason ?? null }),
            };
        }

        case "note":
            return addActivity({ type: "note", text: frame.text });

        case "reply":
            return updateChat(state, chatId, (chat) => ({
                ...chat,
                turns: updateReplyTurn(chat.turns, (turn) => ({
                    ...turn,
                    reply: frame.reply?.trim() || null,
                    gaps: frame.gaps ?? [],
                    questions: (frame.openQuestions ?? []).map(toPlannerQuestion),
                })),
            }));

        case "done":
            return updateChat(state, chatId, (chat) => ({
                ...chat,
                turns: updateReplyTurn(chat.turns, (turn) => ({ ...turn, status: "done" })),
            }));

        case "error":
            return updateChat(state, chatId, (chat) => ({
                ...chat,
                turns: updateReplyTurn(chat.turns, (turn) => failTurn(turn, frame.message)),
            }));
    }
}

/** Whatever the ending, nothing retries the collections the turn was retrying any more. */
export function endPlannerTurn(
    state: PlannerState,
    chatId: PlannerChatId,
    ending: PlannerTurnEnding,
): Partial<PlannerState> {
    const retried =
        getLatestReply(selectChat(state, chatId).turns)?.activity.flatMap((activity) =>
            activity.type === "transientError" ? [activity.collection] : [],
        ) ?? [];

    return {
        ...updateChat(state, chatId, (chat) => ({
            ...chat,
            turns: endReplyTurn(chat.turns, ending),
            abortController: null,
        })),
        plannerRetrying: retried.reduce((retrying, collection) => omitKey(retrying, collection), state.plannerRetrying),
    };
}

/** A stream that ends before the agent says it is done left the turn unfinished, unless the operator disconnected it. */
export function endReplyTurn(turns: PlannerTurn[], { error, isStopped }: PlannerTurnEnding): PlannerTurn[] {
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

export function applyStoredPlan(
    state: PlannerState,
    { plan, proposal }: MigrationPlanGetResponse,
): Partial<PlannerState> {
    const selectedTables = state.plannerSelectedTables ?? [];
    const entries = Object.fromEntries(
        Object.entries(plan.entries ?? {}).map(([name, entry]): [string, PlannerEntry] => [
            name,
            {
                name,
                area: entry.area ?? null,
                status: entry.status ?? "Registered",
                version: entry.version ?? 1,
                rationale: entry.rationale ?? null,
                config: entry.config ?? null,
                tableRef: entry.config
                    ? resolveConfigTables(entry.config, selectedTables, state.plannerDefaultSchema)
                    : null,
                warnings: entry.warnings ?? [],
            },
        ]),
    );
    // A read sent before a frame arrived can answer after it; a turn still streaming keeps what it said since.
    const streamingAreas = new Set(selectWorkingChats(state));

    for (const local of Object.values(state.plannerEntries)) {
        const stored = entries[local.name];
        const isStreaming = local.area !== null && streamingAreas.has(local.area);

        if (isStreaming && (!stored || stored.version < local.version)) {
            entries[local.name] = local;
        }
    }

    const isRejected = (name: string) => entries[name]?.status === "Rejected";

    return {
        plannerProposal: proposal ? resolveProposalTables(proposal, selectedTables) : null,
        plannerConventions: plan.conventions ?? null,
        plannerEntries: entries,
        plannerRejectionIssues: Object.fromEntries(
            Object.entries(state.plannerRejectionIssues).filter(([name]) => isRejected(name)),
        ),
    };
}

/** A turn the agent already finished keeps its outcome: the plan is saved by then. */
function failTurn(turn: PlannerReplyTurn, error: string): PlannerReplyTurn {
    if (turn.status === "done") {
        return { ...turn, activity: [...turn.activity, { type: "lateError", message: error }] };
    }

    return error === STOPPED_TURN_ERROR ? { ...turn, status: "stopped" } : { ...turn, status: "failed", error };
}

function toPlannerQuestion({ question, options, recommended }: OpenQuestion): PlannerQuestion {
    const isRecommendationValid = recommended != null && recommended >= 0 && recommended < options.length;

    return { question, options, recommended: isRecommendationValid ? recommended : null };
}

function updateReplyTurn(turns: PlannerTurn[], update: (turn: PlannerReplyTurn) => PlannerReplyTurn): PlannerTurn[] {
    const last = turns.at(-1);

    if (last?.kind !== "planner") {
        return turns;
    }

    return [...turns.slice(0, -1), update(last)];
}
