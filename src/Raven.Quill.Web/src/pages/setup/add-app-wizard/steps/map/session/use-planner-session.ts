import { useFormContext } from "react-hook-form";
import { toast } from "sonner";
import { api } from "@/api/api";
import type { MigrationFrame, MigrationPlanSaveOutcome } from "@/api/generated/server-api";
import { isApiError, type ApiError } from "@/api/http-client";
import { toError } from "@/components/form/wizard/wizard-step-error";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { type AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { DEFAULT_PROVIDER, resolveConnectionString } from "@/pages/setup/add-app-wizard/connection-string";
import { resolveDefaultSchema } from "@/pages/setup/add-app-wizard/discover-utils";
import { computeConnectKey } from "@/pages/setup/add-app-wizard/steps/connect/use-connect-source-step";
import {
    buildPlanChanges,
    getSavedView,
    groupSaveIssues,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-edits";
import { createTurnId, isAreaNotStartedError } from "@/pages/setup/add-app-wizard/steps/map/session/planner-frames";
import {
    composeAnswersPrompt,
    composeBuildCollectionsPrompt,
    composePlannerPrompt,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-prompts";
import {
    getChatPhase,
    getLatestReply,
    isAreaStarted,
    selectChat,
    selectHasUnsavedEdits,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-state";
import type {
    PlannerAnswer,
    PlannerAnswersTurn,
    PlannerChatId,
    PlannerPrompt,
    PlannerPromptTurn,
    PlannerTurnEnding,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-types";

type FrameStream = (slug: string, signal: AbortSignal) => AsyncGenerator<MigrationFrame>;

export type PlannerSessionContext = {
    getConnection: () => AppFormData["externalConnection"];
    getSelectedTables: () => AppFormData["verifySchema"]["tables"];
};

export type PlannerSession = ReturnType<typeof createPlannerSession>;

/** Only the newest plan/get may land: an older answer would roll back what a later frame moved on. */
let latestPlanRead = 0;

/**
 * Drives the proposal agent's chat and one chat per area; different chats run side by side. Requests carry
 * only the slug, since the server holds the plan and the conversations. A turn's frames move the plan forward.
 */
export function usePlannerSession(): PlannerSession {
    const { getValues } = useFormContext<AppFormData>();

    return createPlannerSession({
        getConnection: () => getValues("externalConnection"),
        getSelectedTables: () => getValues("verifySchema").tables,
    });
}

export function createPlannerSession({ getConnection, getSelectedTables }: PlannerSessionContext) {
    const getStore = () => useSetupWizardStore.getState();

    const openSession = () => {
        const connection = getConnection();
        const provider = connection.provider || DEFAULT_PROVIDER;

        getStore().openPlannerSession({
            slug: connection.slug,
            connectKey: computeConnectKey(connection),
            selectedTables: getSelectedTables(),
            defaultSchema: resolveDefaultSchema(provider, resolveConnectionString(connection)),
        });
    };

    const readPlan = async () => {
        const slug = getStore().plannerSlug;

        if (!slug) {
            return;
        }

        const readNumber = ++latestPlanRead;
        const response = await api.services.setup.migrationPlanGet({ slug });

        if (readNumber === latestPlanRead && getStore().plannerSlug === slug) {
            getStore().receivePlannerPlan(response);
        }
    };

    /** A failed re-read leaves the plan as the frames left it. */
    const syncPlan = () => readPlan().catch((error) => console.error("Could not read the plan again:", error));

    const isCurrentTurn = (chatId: PlannerChatId, controller: AbortController) =>
        selectChat(getStore(), chatId).abortController === controller;

    const hasChangedConventions = (chatId: PlannerChatId) =>
        getLatestReply(selectChat(getStore(), chatId).turns)?.activity.some(
            (activity) => activity.type === "conventions",
        ) ?? false;

    /** Resolves to the error that broke the stream, or null once it ran to its end or a newer turn took over. */
    const receiveFrames = async (
        chatId: PlannerChatId,
        controller: AbortController,
        openStream: () => AsyncGenerator<MigrationFrame>,
    ): Promise<unknown> => {
        try {
            for await (const frame of openStream()) {
                if (!isCurrentTurn(chatId, controller)) {
                    break;
                }

                getStore().receivePlannerFrame(chatId, frame);
            }

            return null;
        } catch (error) {
            return error;
        }
    };

    /** Resolves to the API error that refused the turn, if any. */
    const runTurn = async (
        chatId: PlannerChatId,
        { userTurn, isOpening }: { userTurn: PlannerPromptTurn | PlannerAnswersTurn | null; isOpening: boolean },
        stream: FrameStream,
    ): Promise<ApiError | null> => {
        const slug = getStore().plannerSlug;

        if (!slug) {
            return null;
        }

        const controller = new AbortController();
        getStore().beginPlannerTurn({ chatId, userTurn, controller, isOpening });

        const streamError = await receiveFrames(chatId, controller, () => stream(slug, controller.signal));

        if (!isCurrentTurn(chatId, controller)) {
            return null;
        }

        getStore().endPlannerTurn(chatId, toTurnEnding(streamError, controller.signal.aborted));

        // The proposal's property names follow the conventions, and no proposal frame comes after a change to them.
        if (hasChangedConventions(chatId)) {
            await syncPlan();
        }

        return isApiError(streamError) ? streamError : null;
    };

    /** What the operator wrote goes back to the composer when it did not reach the agent, to send again. */
    const keepDraft = (chatId: PlannerChatId, prompt: PlannerPrompt) => {
        const store = getStore();
        const chat = selectChat(store, chatId);

        store.setPlannerDraft(chatId, chat.draft || prompt.text);
        prompt.mentions.forEach((collection) => store.addPlannerMention(chatId, collection));
    };

    const canStartTurn = (chatId: PlannerChatId) => {
        const store = getStore();

        return (
            !store.plannerIsSaving &&
            !selectHasUnsavedEdits(store) &&
            getChatPhase(selectChat(store, chatId)) !== "working"
        );
    };

    /** Resolves to whether the prompt reached the agent. */
    const askProposal = async (prompt: PlannerPrompt): Promise<boolean> => {
        if (!canStartTurn(null)) {
            return false;
        }

        const turn: PlannerPromptTurn = { kind: "prompt", id: createTurnId(), ...prompt };
        const refusal = await runTurn(null, { userTurn: turn, isOpening: false }, (slug, signal) =>
            api.services.migration.proposalAsk({ slug, prompt: composePlannerPrompt(prompt) }, signal),
        );

        return refusal === null;
    };

    const startArea = async (area: string) => {
        if (!canStartTurn(area)) {
            return;
        }

        getStore().markPlannerAreaStarted(area, true);

        const refusal = await runTurn(area, { userTurn: null, isOpening: true }, (slug, signal) =>
            api.services.migration.areaStart({ slug, area }, signal),
        );

        if (refusal) {
            getStore().markPlannerAreaStarted(area, false);
        }
    };

    /** Resolves to whether the prompt reached the agent. */
    const askArea = async (area: string, prompt: PlannerPrompt): Promise<boolean> => {
        if (!canStartTurn(area)) {
            return false;
        }

        const turn: PlannerPromptTurn = { kind: "prompt", id: createTurnId(), ...prompt };
        const refusal = await runTurn(area, { userTurn: turn, isOpening: false }, (slug, signal) =>
            api.services.migration.areaAsk({ slug, area, prompt: composePlannerPrompt(prompt) }, signal),
        );

        if (refusal) {
            return false;
        }

        // Whether an area started is a guess; the agent's answer settles it. The area builds
        // first, and the prompt, which never reached the agent, can be sent again once it is built.
        const reply = getLatestReply(selectChat(getStore(), area).turns);

        if (reply?.status === "failed" && reply.error && isAreaNotStartedError(reply.error)) {
            getStore().markPlannerAreaStarted(area, false);
            void startArea(area);
            return false;
        }

        return true;
    };

    const takeDraft = (chatId: PlannerChatId): PlannerPrompt | null => {
        const store = getStore();
        const chat = selectChat(store, chatId);
        const text = chat.draft.trim();

        if (!text) {
            return null;
        }

        store.clearPlannerComposer(chatId);

        return { text, mentions: chat.mentions };
    };

    return {
        start: async () => {
            if (!canStartTurn(null)) {
                return;
            }

            const selectedTables = getSelectedTables();
            openSession();

            await runTurn(null, { userTurn: null, isOpening: true }, (slug, signal) =>
                api.services.migration.proposalStart({ slug, selectedTables }, signal),
            );
        },

        askProposal,

        answer: async (chatId: PlannerChatId, answers: PlannerAnswer[]) => {
            const store = getStore();

            if (
                store.plannerIsSaving ||
                selectHasUnsavedEdits(store) ||
                getChatPhase(selectChat(store, chatId)) !== "awaitingAnswers"
            ) {
                return;
            }

            const turn: PlannerAnswersTurn = { kind: "answers", id: createTurnId(), answers };
            const prompt = composeAnswersPrompt(turn);

            await runTurn(chatId, { userTurn: turn, isOpening: false }, (slug, signal) =>
                chatId === null
                    ? api.services.migration.proposalAsk({ slug, prompt }, signal)
                    : api.services.migration.areaAsk({ slug, area: chatId, prompt }, signal),
            );
        },

        /** Builds an area: its first build starts its agent, later ones ask it for what is still missing. */
        buildArea: (area: string, pendingCollections: string[]) => {
            if (!isAreaStarted(getStore(), area)) {
                return startArea(area);
            }

            return pendingCollections.length > 0
                ? askArea(area, { text: composeBuildCollectionsPrompt(pendingCollections), mentions: [] })
                : Promise.resolve();
        },

        askArea,

        sendDraft: async (chatId: PlannerChatId) => {
            if (!canStartTurn(chatId)) {
                return;
            }

            const prompt = takeDraft(chatId);

            if (!prompt) {
                return;
            }

            const isDelivered = chatId === null ? await askProposal(prompt) : await askArea(chatId, prompt);

            if (!isDelivered) {
                keepDraft(chatId, prompt);
            }
        },

        stop: async (chatId: PlannerChatId) => {
            try {
                await getStore().stopPlannerTurn(chatId);
            } catch (error) {
                toast.error(toError(error).message || "Could not stop the planner.");
            }
        },

        save: async () => {
            const store = getStore();
            const slug = store.plannerSlug;
            const { changes, collections } = buildPlanChanges(store.plannerEdits);
            const viewedCollection = store.plannerView.collection;
            const viewedEdit = viewedCollection ? store.plannerEdits[viewedCollection] : undefined;

            if (!slug || changes.length === 0 || store.plannerIsSaving) {
                return;
            }

            store.setPlannerSaving(true);

            try {
                const outcome = await api.services.setup.migrationPlanSave({ slug, changes });
                // The edits stay until the saved plan is read, so the views never fall back to the plan from before.
                const isPlanRead = await readPlan().then(
                    () => true,
                    () => false,
                );

                getStore().receivePlannerSave({ issues: {}, warnings: outcome.warnings, isSaved: true });

                if (viewedCollection && viewedEdit) {
                    getStore().showPlannerView(getSavedView(viewedCollection, viewedEdit));
                }

                if (isPlanRead) {
                    toast.success("Saved to the plan.");
                } else {
                    toast.warning("Saved to the plan, but it could not be loaded again. Reload the page to see it.");
                }
            } catch (error) {
                if (isApiError<MigrationPlanSaveOutcome>(error) && error.status === 422 && error.details?.issues) {
                    getStore().receivePlannerSave({
                        issues: groupSaveIssues(error.details.issues, collections),
                        warnings: error.details.warnings ?? [],
                        isSaved: false,
                    });
                } else {
                    toast.error(toError(error).message || "Could not save the plan.");
                }
            } finally {
                getStore().setPlannerSaving(false);
            }
        },
    };
}

function toTurnEnding(streamError: unknown, isAborted: boolean): PlannerTurnEnding {
    if (streamError === null) {
        return { error: null, isStopped: false };
    }

    return isAborted ? { error: null, isStopped: true } : { error: toError(streamError).message, isStopped: false };
}
