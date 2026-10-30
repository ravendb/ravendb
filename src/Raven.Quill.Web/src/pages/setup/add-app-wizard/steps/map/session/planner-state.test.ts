import { describe, expect, it } from "vitest";
import { createReplyTurn } from "@/pages/setup/add-app-wizard/steps/map/session/planner-frames";
import {
    EMPTY_CHAT,
    getChatPhase,
    initialPlannerState,
    isAreaStarted,
    selectAreConventionsLocked,
    selectCanEditPlan,
    selectWorkingChats,
    type PlannerState,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-state";
import type { PlannerChat, PlannerReplyTurn } from "@/pages/setup/add-app-wizard/steps/map/session/planner-types";

const chatWith = (turn: Partial<PlannerReplyTurn>): PlannerChat => ({
    ...EMPTY_CHAT,
    turns: [{ ...createReplyTurn(false), ...turn }],
});

const proposal: PlannerState["plannerProposal"] = {
    areas: [
        { area: "Sales", why: null, collections: [] },
        { area: "Billing", why: null, collections: [] },
    ],
    dropped: [],
    enables: [],
};

describe("getChatPhase", () => {
    it("follows the chat's latest reply", () => {
        expect(getChatPhase(EMPTY_CHAT)).toBe("idle");
        expect(getChatPhase(chatWith({ status: "working" }))).toBe("working");
        expect(getChatPhase(chatWith({ status: "done" }))).toBe("ready");
        expect(getChatPhase(chatWith({ status: "failed" }))).toBe("ready");
    });

    it("waits on the operator while a finished turn's questions stand", () => {
        const questions = [{ question: "Casing?", options: ["camelCase"], recommended: 0 }];

        expect(getChatPhase(chatWith({ status: "done", questions }))).toBe("awaitingAnswers");
        expect(getChatPhase(chatWith({ status: "stopped", questions }))).toBe("ready");
    });
});

describe("selectWorkingChats", () => {
    it("lists the proposal agent and every area agent at work, side by side", () => {
        const state: PlannerState = {
            ...initialPlannerState,
            plannerProposalChat: chatWith({ status: "working" }),
            plannerAreaChats: {
                Sales: chatWith({ status: "working" }),
                Billing: chatWith({ status: "done" }),
            },
        };

        expect(selectWorkingChats(state)).toEqual([null, "Sales"]);
        expect(selectCanEditPlan({ ...state, plannerProposal: proposal })).toBe(false);
    });
});

describe("isAreaStarted", () => {
    it("knows the areas this session started, and guesses the others from what their agents mapped", () => {
        const state: PlannerState = {
            ...initialPlannerState,
            plannerProposal: proposal,
            plannerStartedAreas: ["Sales"],
            plannerEntries: {
                Invoices: {
                    name: "Invoices",
                    area: "Billing",
                    status: "Rejected",
                    version: 0,
                    rationale: null,
                    config: null,
                    tableRef: null,
                    warnings: [],
                },
            },
        };

        expect(isAreaStarted(state, "Sales")).toBe(true);
        expect(isAreaStarted(state, "Billing")).toBe(true);
        expect(isAreaStarted(state, "Shipping")).toBe(false);
    });

    it("locks the conventions for good once any area started", () => {
        const state: PlannerState = { ...initialPlannerState, plannerProposal: proposal };

        expect(selectAreConventionsLocked(state)).toBe(false);
        expect(selectAreConventionsLocked({ ...state, plannerStartedAreas: ["Billing"] })).toBe(true);
    });
});
