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
    phase: "ready",
    consent: grantedConsent,
    builtCount: 2,
    unbuiltCount: 0,
    hasMapping: false,
    isMappingFromPlan: false,
    isRemoving: false,
};

describe("getPlannerNextState", () => {
    it("applies the built collections", () => {
        expect(getPlannerNextState(ready)).toEqual({
            isNextDisabled: false,
            note: "2 collections go to the mapping.",
            label: "Apply to mapping",
        });
        expect(getPlannerNextState({ ...ready, builtCount: 1 }).note).toBe("1 collection goes to the mapping.");
    });

    it("says how many unbuilt collections are left out", () => {
        expect(getPlannerNextState({ ...ready, unbuiltCount: 1 }).note).toBe(
            "2 collections go to the mapping, 1 unbuilt is left out.",
        );
        expect(getPlannerNextState({ ...ready, unbuiltCount: 3 }).note).toBe(
            "2 collections go to the mapping, 3 unbuilt are left out.",
        );
    });

    it("says when applying replaces the current mapping, and keeps one these collections already produced", () => {
        expect(getPlannerNextState({ ...ready, hasMapping: true }).note).toBe(
            "2 collections replace the current mapping.",
        );
        expect(getPlannerNextState({ ...ready, hasMapping: true, isMappingFromPlan: true })).toEqual({
            isNextDisabled: false,
            note: "Next keeps the current mapping.",
            label: "Next",
        });
    });

    it("applies what is built without waiting for the planner's questions", () => {
        expect(getPlannerNextState({ ...ready, phase: "awaitingAnswers" })).toEqual({
            isNextDisabled: false,
            note: "2 collections go to the mapping. The planner's questions stay unanswered.",
            label: "Apply to mapping",
        });
    });

    it("holds back while a removal is being saved", () => {
        expect(getPlannerNextState({ ...ready, isRemoving: true })).toMatchObject({
            isNextDisabled: true,
            note: "Removing a collection from the plan.",
        });
    });

    it("lets a mapping the form already holds through when the planner has nothing to apply", () => {
        const keeps = { isNextDisabled: false, note: "Next keeps the current mapping.", label: "Next" };

        expect(getPlannerNextState({ ...ready, phase: "idle", hasMapping: true })).toEqual(keeps);
        expect(getPlannerNextState({ ...ready, phase: "failed", hasMapping: true })).toEqual(keeps);
        expect(getPlannerNextState({ ...ready, builtCount: 0, hasMapping: true })).toEqual(keeps);
    });

    it("waits for the AI terms before the planner has started", () => {
        expect(
            getPlannerNextState({
                ...ready,
                phase: "idle",
                consent: { ...grantedConsent, isGranted: false, isConsentRequired: true, isBlocked: true },
            }),
        ).toEqual({
            isNextDisabled: true,
            note: "Accept the RavenDB AI Assistant Terms of Use to continue with AI. Or use manual mapping.",
            label: "Apply to mapping",
        });
    });

    it("points to manual mapping when the AI service is out of reach", () => {
        const consent = { ...grantedConsent, isGranted: false, isBlocked: true, unavailableReason: "Quota used up." };

        expect(getPlannerNextState({ ...ready, phase: "idle", consent }).note).toBe(
            "Quota used up. Or use manual mapping.",
        );
    });

    it("waits for the planner while it works, whatever the form holds", () => {
        expect(getPlannerNextState({ ...ready, phase: "opening" }).note).toBe(
            "The planner is working on a first plan.",
        );
        expect(getPlannerNextState({ ...ready, phase: "working", hasMapping: true })).toMatchObject({
            isNextDisabled: true,
            note: "The planner is still working.",
        });
    });

    it("waits for the planner's questions to be answered while nothing is built", () => {
        expect(getPlannerNextState({ ...ready, phase: "awaitingAnswers", builtCount: 0 }).note).toBe(
            "Answer the planner's questions, or skip them to accept its recommendations.",
        );
    });

    it("holds back until something is built", () => {
        expect(getPlannerNextState({ ...ready, phase: "failed" }).isNextDisabled).toBe(true);
        expect(getPlannerNextState({ ...ready, builtCount: 0 })).toEqual({
            isNextDisabled: true,
            note: "Nothing is built yet. Ask the planner to build the collections, or use manual mapping.",
            label: "Apply to mapping",
        });
    });
});
