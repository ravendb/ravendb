import { useRef } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { useFormContext } from "react-hook-form";
import { api } from "@/api/api";
import { isApiError } from "@/api/http-client";
import type { MigrationFrame } from "@/api/custom-services/migration-service";
import { type AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { useSetupWizardStore, type PlannerMessage } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { splitOpenQuestions } from "@/pages/setup/add-app-wizard/steps/map/planner-questions-utils";

export const isPlannerConsentRefusal = (error: unknown) => isApiError(error) && error.status === 401;

let nextMessageId = 0;

const message = (role: PlannerMessage["role"], text: string): PlannerMessage => ({
    id: `planner-${(nextMessageId += 1)}`,
    role,
    text,
});

export function consumePlannerFrame(frame: MigrationFrame) {
    const store = useSetupWizardStore.getState();

    switch (frame.type) {
        case "proposal":
            store.setPlannerProposal({
                areas: frame.areas,
                collections: frame.collections,
                dropped: frame.dropped,
                enables: frame.enables,
            });
            store.appendPlannerMessage(message("tool", `Proposed ${frame.collections.length} collections.`));
            break;

        case "collection":
            store.upsertPlannerCollection({
                collection: frame.collection,
                version: frame.version,
                status: frame.status,
                rationale: frame.rationale,
                config: frame.config,
                warnings: frame.warnings,
            });
            store.appendPlannerMessage(message("tool", `${frame.status} ${frame.collection}`));
            break;

        case "rejected":
            // Kept as a card rather than dropped: a rejection the operator cannot see looks
            // identical to a collection the agent never attempted.
            store.upsertPlannerCollection({
                collection: frame.collection,
                version: 0,
                status: "rejected",
                warnings: [],
                errors: frame.errors,
            });
            store.appendPlannerMessage(message("tool", `rejected ${frame.collection}`));
            break;

        case "removed":
            if (frame.collection) {
                store.removePlannerCollection(frame.collection);
                store.appendPlannerMessage(message("tool", `removed ${frame.collection}`));
            }
            break;

        case "conventions":
            store.appendPlannerMessage(
                message(
                    "tool",
                    `conventions: ${frame.propertyCase}${frame.propertyLanguage ? `, ${frame.propertyLanguage}` : ""}` +
                        (frame.mustReEmit.length > 0 ? ` — re-emitting ${frame.mustReEmit.join(", ")}` : ""),
                ),
            );
            break;

        case "note":
            store.appendPlannerMessage(message("tool", frame.text));
            break;

        case "reply": {
            const { pickable, freeForm } = splitOpenQuestions(frame.openQuestions);
            store.appendPlannerMessage(message("agent", replyText(frame, freeForm)));
            store.setPlannerQuestions(pickable);
            break;
        }

        case "done":
            store.setPlannerConversationId(frame.conversationId);
            break;

        case "error":
            store.appendPlannerMessage(message("error", frame.message));
            store.setPlannerQuestions([]);
            break;
    }
}

/**
 * Drives one planning conversation. Frames are split by audience: the agent's own words go to the
 * transcript, everything it registered goes to the results pane, and each tool call leaves a marker
 * in the transcript so the two read in the same order.
 */
export function usePlannerSession() {
    const queryClient = useQueryClient();
    const { getValues } = useFormContext<AppFormData>();
    const abortRef = useRef<AbortController | null>(null);

    const isStreaming = useSetupWizardStore((state) => state.isPlannerStreaming);
    const conversationId = useSetupWizardStore((state) => state.plannerConversationId);

    const run = async (frames: (signal: AbortSignal) => AsyncGenerator<MigrationFrame>) => {
        const controller = new AbortController();
        abortRef.current = controller;
        useSetupWizardStore.getState().setIsPlannerStreaming(true);

        try {
            for await (const frame of frames(controller.signal)) {
                consumePlannerFrame(frame);
            }
        } catch (error) {
            if (isPlannerConsentRefusal(error)) {
                void queryClient.invalidateQueries({ queryKey: api.queries.assistant.consent().queryKey });
            }

            if (!controller.signal.aborted) {
                const store = useSetupWizardStore.getState();
                store.appendPlannerMessage(message("error", error instanceof Error ? error.message : String(error)));
                store.setPlannerQuestions([]);
            }
        } finally {
            abortRef.current = null;
            useSetupWizardStore.getState().setIsPlannerStreaming(false);
        }
    };

    return {
        isStreaming,
        conversationId,

        start: () => {
            const store = useSetupWizardStore.getState();
            const selectedTables = getValues("verifySchema").tables;
            store.resetPlannerState();
            store.setPlannerSelectedTables(selectedTables);

            return run((signal) =>
                api.services.migration.start({ slug: getValues("externalConnection").slug, selectedTables }, signal),
            );
        },

        ask: (prompt: string) => {
            const store = useSetupWizardStore.getState();

            if (!store.plannerConversationId) {
                return;
            }

            store.appendPlannerMessage(message("user", prompt));
            store.setPlannerQuestions([]);

            return run((signal) =>
                api.services.migration.ask(
                    {
                        slug: getValues("externalConnection").slug,
                        conversationId: store.plannerConversationId!,
                        prompt,
                    },
                    signal,
                ),
            );
        },

        stop: () => abortRef.current?.abort(),
    };
}

/**
 * The reply carries gaps and open questions the transcript should show alongside the prose. Only
 * the questions without answers to pick from are listed; the rest are asked in the picker.
 */
function replyText(frame: Extract<MigrationFrame, { type: "reply" }>, freeFormQuestions: string[]): string {
    const sections = [frame.reply?.trim() ?? ""];

    if (frame.gaps.length > 0) {
        sections.push(`**Gaps**\n\n${frame.gaps.map((gap) => `- ${gap}`).join("\n")}`);
    }

    if (freeFormQuestions.length > 0) {
        sections.push(`**Open questions**\n\n${freeFormQuestions.map((q) => `- ${q}`).join("\n")}`);
    }

    return sections.filter(Boolean).join("\n\n");
}

export function useRemovePlannerCollection() {
    const { getValues } = useFormContext<AppFormData>();

    return async (collection: string) => {
        const store = useSetupWizardStore.getState();

        if (!store.plannerConversationId) {
            return;
        }

        try {
            await api.services.migration.removeCollection({
                slug: getValues("externalConnection").slug,
                conversationId: store.plannerConversationId,
                collection,
            });
            useSetupWizardStore.getState().removePlannerCollection(collection);
        } catch (error) {
            useSetupWizardStore
                .getState()
                .appendPlannerMessage(message("error", error instanceof Error ? error.message : String(error)));
        }
    };
}
