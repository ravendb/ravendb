import { describeAiConsentBlock, type AiConsentState } from "@/components/ai-consent/use-ai-consent";
import { formatCount } from "@/lib/format";

export type PlannerNextState = { isNextDisabled: boolean; note: string };

export type PlannerNextInput = {
    consent: AiConsentState;
    isManualMapping: boolean;
    hasSession: boolean;
    isWorking: boolean;
    isAwaitingAnswers: boolean;
    isSaving: boolean;
    unsavedCount: number;
    builtCount: number;
    /** Failed collections count as not built. */
    unbuiltCount: number;
};

const GOES_ON: PlannerNextState = { isNextDisabled: false, note: "" };

export function getPlannerNextState(input: PlannerNextInput): PlannerNextState {
    const blocked = (note: string): PlannerNextState => ({ isNextDisabled: true, note });

    if (input.isManualMapping) {
        return GOES_ON;
    }

    if (!input.hasSession) {
        return blocked(
            describeAiConsentBlock(input.consent, "Or use manual mapping.") ??
                "Start the planner, or use manual mapping.",
        );
    }

    if (input.isWorking) {
        return blocked("A conversation is still working. Wait for it to finish, or stop it.");
    }

    if (input.isSaving) {
        return blocked("Saving the plan…");
    }

    if (input.unsavedCount > 0) {
        return blocked("Save or discard your unsaved changes first.");
    }

    if (input.builtCount === 0) {
        return blocked(
            input.isAwaitingAnswers
                ? "Answer the planner's questions first."
                : "Build at least one area first. Only built collections go to the app.",
        );
    }

    const built = `${formatCount(input.builtCount, "collection")} ${input.builtCount === 1 ? "goes" : "go"} to the app`;
    const leftOut =
        input.unbuiltCount > 0
            ? `, ${input.unbuiltCount} not built ${input.unbuiltCount === 1 ? "is" : "are"} left out`
            : "";

    return { isNextDisabled: false, note: `${built}${leftOut}.` };
}
