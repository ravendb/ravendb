import { afterEach, describe, expect, it } from "vitest";
import { ApiError } from "@/api/http-client";
import { useSetupWizardStore, type PlannerCollection } from "@/pages/setup/add-app-wizard/app-wizard-store";
import {
    consumePlannerFrame,
    isPlannerConsentRefusal,
} from "@/pages/setup/add-app-wizard/steps/map/use-planner-session";

const collection = (name: string, status: PlannerCollection["status"]): PlannerCollection => ({
    collection: name,
    version: 1,
    status,
    warnings: [],
});

describe("isPlannerConsentRefusal", () => {
    it("treats a 401 from the planner as a refusal the consent check has to answer", () => {
        expect(isPlannerConsentRefusal(new ApiError("consent required", 401, undefined))).toBe(true);
    });

    it("leaves other failures alone", () => {
        expect(isPlannerConsentRefusal(new ApiError("bad request", 400, undefined))).toBe(false);
        expect(isPlannerConsentRefusal(new ApiError("unreachable", 502, undefined))).toBe(false);
        expect(isPlannerConsentRefusal(new Error("network"))).toBe(false);
    });
});

describe("dismissing a rejected collection", () => {
    afterEach(() => useSetupWizardStore.getState().resetPlannerState());

    it("removes only that card and keeps what the planner registered", () => {
        const store = useSetupWizardStore.getState();
        store.upsertPlannerCollection(collection("Loans", "registered"));
        store.upsertPlannerCollection(collection("LoanProducts", "rejected"));

        useSetupWizardStore.getState().removePlannerCollection("LoanProducts");

        expect(Object.keys(useSetupWizardStore.getState().plannerCollections)).toEqual(["Loans"]);
    });
});

describe("a turn that fails after its reply", () => {
    afterEach(() => useSetupWizardStore.getState().resetPlannerState());

    it("drops the reply's questions, since there is no conversation left to answer them in", () => {
        consumePlannerFrame({
            type: "reply",
            reply: "Here is the plan.",
            gaps: [],
            openQuestions: [{ question: "Embed lines?", options: ["Yes", "No"], recommended: 0 }],
        });
        expect(useSetupWizardStore.getState().plannerQuestions).toHaveLength(1);

        consumePlannerFrame({ type: "error", message: "Could not store the schema (error id: 'x')" });

        const state = useSetupWizardStore.getState();
        expect(state.plannerQuestions).toEqual([]);
        expect(state.plannerConversationId).toBeNull();
        expect(state.plannerMessages.map((m) => m.role)).toEqual(["agent", "error"]);
    });
});
