import { useFormContext } from "react-hook-form";
import { api } from "@/api/api";
import type { MigrationFrame } from "@/api/generated/server-api";
import { toError } from "@/components/form/wizard/wizard-step-error";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { type AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { haveSameTables } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-utils";
import { createTurnId } from "@/pages/setup/add-app-wizard/steps/map/session/planner-frames";
import {
    composeAnswersPrompt,
    composePlannerPrompt,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-prompts";
import { selectPlannerPhase } from "@/pages/setup/add-app-wizard/steps/map/session/planner-state";
import type {
    PlannerAnswer,
    PlannerAnswersTurn,
    PlannerPrompt,
    PlannerPromptTurn,
    SelectedTables,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-types";

type FrameStream = (signal: AbortSignal) => AsyncGenerator<MigrationFrame>;

export type PlannerSessionContext = {
    getSlug: () => string;
    getSelectedTables: () => SelectedTables;
};

export type PlannerSession = ReturnType<typeof createPlannerSession>;

/**
 * Drives one planning conversation. Each turn streams frames into the store, which folds the planner's
 * words into the transcript and everything it registered into the plan. One turn runs at a time, and
 * none starts while the planner waits on its questions.
 */
export function usePlannerSession(): PlannerSession {
    const { getValues } = useFormContext<AppFormData>();

    return createPlannerSession({
        getSlug: () => getValues("externalConnection").slug,
        getSelectedTables: () => getValues("verifySchema").tables,
    });
}

export function createPlannerSession({ getSlug, getSelectedTables }: PlannerSessionContext) {
    const askInConversation = (userTurn: PlannerPromptTurn | PlannerAnswersTurn, prompt: string) => {
        const store = useSetupWizardStore.getState();
        const conversationId = store.plannerConversationId;

        if (conversationId === null) {
            return Promise.resolve();
        }

        const controller = new AbortController();
        store.beginPlannerTurn({ userTurn, controller });

        return streamTurn(controller, (signal) =>
            api.services.migration.ask({ slug: getSlug(), conversationId, prompt }, signal),
        );
    };

    const ask = (prompt: PlannerPrompt) => {
        if (selectPlannerPhase(useSetupWizardStore.getState()) !== "ready") {
            return Promise.resolve();
        }

        return askInConversation({ kind: "prompt", id: createTurnId(), ...prompt }, composePlannerPrompt(prompt));
    };

    /** Sends what the composer holds. While the planner works or waits on its questions, it stays there. */
    const sendDraft = () => {
        if (selectPlannerPhase(useSetupWizardStore.getState()) !== "ready") {
            return Promise.resolve();
        }

        const prompt = takeDraft();

        return prompt ? ask(prompt) : Promise.resolve();
    };

    /** The operator may have changed what the session was opened on while it opened, which ends the session. */
    const isOpenedOnCurrentInputs = (selectedTables: SelectedTables) =>
        useSetupWizardStore.getState().plannerSelectedTables === selectedTables &&
        haveSameTables(getSelectedTables(), selectedTables);

    return {
        /** Opens the session on the selected tables. A session that already started is left alone. */
        start: async () => {
            const store = useSetupWizardStore.getState();

            if (selectPlannerPhase(store) !== "idle") {
                return;
            }

            const selectedTables = getSelectedTables();
            const controller = new AbortController();
            store.openPlannerSession({ selectedTables, controller });

            await streamTurn(controller, (signal) =>
                api.services.migration.start({ slug: getSlug(), selectedTables }, signal),
            );

            // Instructions written during the opening go in once the first plan is ready, or with the
            // answers when the planner opens with questions. A session about to be started over keeps
            // them for the next one.
            if (isOpenedOnCurrentInputs(selectedTables)) {
                await sendDraft();
            }
        },

        sendDraft,

        ask,

        /** Answers the open questions, together with anything the operator wrote for the planner meanwhile. */
        answer: (answers: PlannerAnswer[], isSkipped: boolean) => {
            if (selectPlannerPhase(useSetupWizardStore.getState()) !== "awaitingAnswers") {
                return Promise.resolve();
            }

            const turn: PlannerAnswersTurn = {
                kind: "answers",
                id: createTurnId(),
                answers,
                isSkipped,
                instructions: takeDraft(),
            };

            return askInConversation(turn, composeAnswersPrompt(turn));
        },

        stop: () => useSetupWizardStore.getState().stopPlannerTurn(),
    };
}

/**
 * Starting over, or leaving the wizard, replaces the session under a running turn; whatever that turn
 * still delivers belongs to a conversation that is gone.
 */
async function streamTurn(controller: AbortController, stream: FrameStream) {
    const isCurrent = () => useSetupWizardStore.getState().plannerAbortController === controller;

    try {
        for await (const frame of stream(controller.signal)) {
            if (!isCurrent()) {
                return;
            }

            useSetupWizardStore.getState().receivePlannerFrame(frame);
        }

        if (isCurrent()) {
            useSetupWizardStore.getState().endPlannerTurn({ error: null, isStopped: false });
        }
    } catch (error) {
        if (!isCurrent()) {
            return;
        }

        const store = useSetupWizardStore.getState();
        store.endPlannerTurn(
            controller.signal.aborted
                ? { error: null, isStopped: true }
                : { error: toError(error).message, isStopped: false },
        );
    }
}

function takeDraft(): PlannerPrompt | null {
    const store = useSetupWizardStore.getState();
    const text = store.plannerDraft.trim();

    if (!text) {
        return null;
    }

    const prompt = { text, mentions: store.plannerMentions };
    store.clearPlannerComposer();

    return prompt;
}
