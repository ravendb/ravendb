import { describe, expect, it } from "vitest";
import type { AiConsentState } from "@/components/ai-consent/use-ai-consent";
import { getPlannerNextState, type PlannerNextInput } from "@/pages/setup/add-app-wizard/steps/map/planner-next";

const grantedConsent: AiConsentState = {
    isPending: false,
    isGranted: true,
    isConsentRequired: false,
    isBlocked: false,
    unavailableReason: undefined,
    isRetryable: false,
    recheck: () => {},
};

const ready: PlannerNextInput = {
    consent: grantedConsent,
    isManualMapping: false,
    hasSession: true,
    isWorking: false,
    isAwaitingAnswers: false,
    isSaving: false,
    unsavedCount: 0,
    builtCount: 2,
    unbuiltCount: 0,
};

describe("getPlannerNextState", () => {
    it("creates the app from what is built, and says what is left out", () => {
        expect(getPlannerNextState(ready)).toEqual({ isNextDisabled: false, note: "2 collections go to the app." });
        expect(getPlannerNextState({ ...ready, builtCount: 1, unbuiltCount: 1 })).toEqual({
            isNextDisabled: false,
            note: "1 collection goes to the app, 1 not built is left out.",
        });
    });

    it("waits while any conversation works, a save runs, or edits are unsaved", () => {
        expect(getPlannerNextState({ ...ready, isWorking: true }).isNextDisabled).toBe(true);
        expect(getPlannerNextState({ ...ready, isSaving: true }).isNextDisabled).toBe(true);
        expect(getPlannerNextState({ ...ready, unsavedCount: 2 })).toEqual({
            isNextDisabled: true,
            note: "Save or discard your unsaved changes first.",
        });
    });

    it("needs a built collection, and the planner's answers first while it waits on them", () => {
        expect(getPlannerNextState({ ...ready, builtCount: 0 }).note).toBe(
            "Build at least one area first. Only built collections go to the app.",
        );
        expect(getPlannerNextState({ ...ready, builtCount: 0, isAwaitingAnswers: true }).note).toBe(
            "Answer the planner's questions first.",
        );
    });

    it("points at the way on before the planner starts", () => {
        expect(getPlannerNextState({ ...ready, hasSession: false })).toEqual({
            isNextDisabled: true,
            note: "Start the planner, or use manual mapping.",
        });
    });

    it("goes on to the editor once the operator chose manual mapping", () => {
        expect(getPlannerNextState({ ...ready, hasSession: false, isManualMapping: true })).toEqual({
            isNextDisabled: false,
            note: "",
        });
    });
});
