import { describe, expect, it } from "vitest";
import type { PlannerCollection } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { getPlannerNextBlock } from "@/pages/setup/add-app-wizard/steps/map/use-planner-next-block";

const registered = (name: string): PlannerCollection => ({
    collection: name,
    version: 1,
    status: "registered",
    warnings: [],
});

const idle = {
    isStreaming: false,
    hasQuestions: false,
    hasConversation: true,
    hasMappedTables: false,
    collections: [registered("Orders")],
    deselected: {},
};

describe("getPlannerNextBlock", () => {
    it("lets a finished session with a kept collection through", () => {
        expect(getPlannerNextBlock(idle)).toEqual({ isNextDisabled: false });
    });

    it("blocks collections left behind by a session that never finished", () => {
        const block = getPlannerNextBlock({ ...idle, hasConversation: false });

        expect(block.isNextDisabled).toBe(true);
        expect(block.nextDisabledReason).toContain("did not finish");
    });

    it("lets a manual mapping through when the planner registered nothing", () => {
        expect(
            getPlannerNextBlock({ ...idle, hasConversation: false, collections: [], hasMappedTables: true }),
        ).toEqual({ isNextDisabled: false });
    });

    it("blocks when every registered collection was deselected", () => {
        expect(getPlannerNextBlock({ ...idle, deselected: { Orders: true } }).isNextDisabled).toBe(true);
    });
});
