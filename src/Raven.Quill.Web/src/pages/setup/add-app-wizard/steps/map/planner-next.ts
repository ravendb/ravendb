import { describeAiConsentBlock, type AiConsentState } from "@/components/ai-consent/use-ai-consent";
import { formatCount } from "@/lib/format";
import type { PlannerPhase } from "@/pages/setup/add-app-wizard/steps/map/session/planner-state";

export type PlannerNextState = { isNextDisabled: boolean; note: string; label: string };

export type PlannerNextInput = {
    phase: PlannerPhase;
    consent: AiConsentState;
    builtCount: number;
    /** Suggested and failed collections, which applying leaves out. */
    unbuiltCount: number;
    /** The editor already holds a mapping, which Next can carry on with. */
    hasMapping: boolean;
    /** That mapping came from applying exactly the built collections, so applying them again would only undo edits. */
    isMappingFromPlan: boolean;
    /** A removal is still being saved, and applying now could hand over the collection being removed. */
    isRemoving: boolean;
};

const APPLY_LABEL = "Apply to mapping";

const NEXT_LABEL = "Next";

const KEEPS_MAPPING: PlannerNextState = {
    isNextDisabled: false,
    note: "Next keeps the current mapping.",
    label: NEXT_LABEL,
};

export function getPlannerNextState(input: PlannerNextInput): PlannerNextState {
    const blocked = (note: string): PlannerNextState => ({ isNextDisabled: true, note, label: APPLY_LABEL });

    switch (input.phase) {
        case "opening":
            return blocked("The planner is working on a first plan.");
        case "working":
            return blocked("The planner is still working.");
    }

    if (input.isRemoving) {
        return blocked("Removing a collection from the plan.");
    }

    const isAwaitingAnswers = input.phase === "awaitingAnswers";

    if ((input.phase === "ready" || isAwaitingAnswers) && input.builtCount > 0) {
        if (input.isMappingFromPlan) {
            return KEEPS_MAPPING;
        }

        const collections = formatCount(input.builtCount, "collection");
        const verb = input.builtCount === 1 ? "goes" : "go";
        const replaces = input.builtCount === 1 ? "replaces" : "replace";
        const applied = input.hasMapping
            ? `${collections} ${replaces} the current mapping`
            : `${collections} ${verb} to the mapping`;
        const leftOut = input.unbuiltCount > 0 ? `, ${describeLeftOut(input.unbuiltCount)}` : "";
        const unanswered = isAwaitingAnswers ? " The planner's questions stay unanswered." : "";

        return { isNextDisabled: false, note: `${applied}${leftOut}.${unanswered}`, label: APPLY_LABEL };
    }

    if (input.hasMapping) {
        return KEEPS_MAPPING;
    }

    if (isAwaitingAnswers) {
        return blocked("Answer the planner's questions, or skip them to accept its recommendations.");
    }

    if (input.phase === "idle") {
        return blocked(
            describeAiConsentBlock(input.consent, "Or use manual mapping.") ??
                "The planner is working on a first plan.",
        );
    }

    if (input.phase === "failed") {
        return blocked("The planner session ended. Start over, or use manual mapping.");
    }

    return blocked("Nothing is built yet. Ask the planner to build the collections, or use manual mapping.");
}

function describeLeftOut(unbuiltCount: number): string {
    return `${unbuiltCount} unbuilt ${unbuiltCount === 1 ? "is" : "are"} left out`;
}
