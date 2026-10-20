import { describe, expect, it } from "vitest";
import { createReplyTurn } from "@/pages/setup/add-app-wizard/steps/map/session/planner-frames";
import {
    initialPlannerState,
    selectBuiltCollections,
    selectIsPlanSettled,
    selectPendingQuestions,
    selectPlannerPhase,
    withoutRejections,
    type PlannerState,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-state";
import type {
    PlannerQuestion,
    PlannerReplyTurn,
    PlannerTurn,
    RegisteredCollection,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-types";

const question: PlannerQuestion = { question: "Embed lines?", options: ["Yes", "No"], recommended: 0 };

function reply(overrides: Partial<PlannerReplyTurn> = {}): PlannerReplyTurn {
    return { ...createReplyTurn(false), ...overrides };
}

function state(turns: PlannerTurn[], overrides: Partial<PlannerState> = {}): PlannerState {
    return { ...initialPlannerState, plannerTurns: turns, plannerConversationId: "planner/1", ...overrides };
}

const built = (name: string): RegisteredCollection => ({
    name,
    outcome: "registered",
    version: 1,
    registrationNumber: 1,
    rationale: null,
    config: null,
    tableRef: null,
    previousConfig: null,
    warnings: [],
    rejectionErrors: [],
});

describe("selectPlannerPhase", () => {
    it("is idle until the session opens", () => {
        expect(selectPlannerPhase(initialPlannerState)).toBe("idle");
    });

    it("tells the opening turn from a later one", () => {
        expect(selectPlannerPhase(state([createReplyTurn(true)], { plannerConversationId: null }))).toBe("opening");
        expect(selectPlannerPhase(state([reply()]))).toBe("working");
    });

    it("is ready as soon as the planner says the turn is done, even while its stream is still closing", () => {
        expect(
            selectPlannerPhase(state([reply({ status: "done" })], { plannerAbortController: new AbortController() })),
        ).toBe("ready");
    });

    it("waits on the questions a finished turn ended with", () => {
        expect(selectPlannerPhase(state([reply({ status: "done", questions: [question] })]))).toBe("awaitingAnswers");
    });

    it("leaves nothing to answer when the turn did not finish", () => {
        expect(selectPlannerPhase(state([reply({ status: "stopped", questions: [question] })]))).toBe("ready");
        expect(selectPendingQuestions(state([reply({ status: "failed", questions: [question] })]))).toEqual([]);
    });

    it("has failed when the opening ended without a conversation", () => {
        const failedOpening: PlannerReplyTurn = { ...createReplyTurn(true), status: "failed" };

        expect(selectPlannerPhase(state([failedOpening], { plannerConversationId: null }))).toBe("failed");
    });
});

describe("selectBuiltCollections", () => {
    it("leaves out the collections the planner failed to register", () => {
        expect(
            selectBuiltCollections({
                ...initialPlannerState,
                plannerCollections: {
                    Orders: built("Orders"),
                    Products: built("Products"),
                    Invoices: { name: "Invoices", outcome: "rejected", errors: [], config: null, issues: [] },
                },
            }).map((collection) => collection.name),
        ).toEqual(["Orders", "Products"]);
    });
});

describe("selectIsPlanSettled", () => {
    it("holds the plan still once the planner finished, also while it waits on its questions", () => {
        expect(selectIsPlanSettled(state([reply({ status: "done" })]))).toBe(true);
        expect(selectIsPlanSettled(state([reply({ status: "done", questions: [question] })]))).toBe(true);
        expect(selectIsPlanSettled(state([reply()]))).toBe(false);
        expect(selectIsPlanSettled(initialPlannerState)).toBe(false);
    });
});

describe("withoutRejections", () => {
    it("lets go of the rejected changes named, and leaves every other collection as it was", () => {
        const orders = { ...built("Orders"), rejectionErrors: ["Total is mapped twice."] };
        const products = { ...built("Products"), rejectionErrors: ["Price is mapped twice."] };

        const update = withoutRejections(state([], { plannerCollections: { Orders: orders, Products: products } }), [
            "Orders",
        ]);

        expect(update.plannerCollections?.Orders).toMatchObject({ name: "Orders", rejectionErrors: [] });
        expect(update.plannerCollections?.Products).toBe(products);
    });
});
