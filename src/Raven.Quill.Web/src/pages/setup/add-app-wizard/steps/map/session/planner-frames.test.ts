import { describe, expect, it } from "vitest";
import type { CdcSinkTableConfig, MigrationFrame, MigrationPlanGetResponse } from "@/api/generated/server-api";
import {
    applyPlannerFrame,
    applyStoredPlan,
    createReplyTurn,
    endPlannerTurn,
    endReplyTurn,
    isAreaNotStartedError,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-frames";
import {
    EMPTY_CHAT,
    initialPlannerState,
    selectChat,
    type PlannerState,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-state";
import type {
    PlannerChatId,
    PlannerEntry,
    PlannerReplyTurn,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-types";

const ORDERS = { sourceTableSchema: "dbo", sourceTableName: "Orders" };

const ordersConfig = (column: string): CdcSinkTableConfig => ({
    collectionName: "Orders",
    sourceTableSchema: "dbo",
    sourceTableName: "Orders",
    columns: [{ column, name: column.toLowerCase() }],
});

const ordersEntry = (overrides: Partial<PlannerEntry> = {}): PlannerEntry => ({
    name: "Orders",
    area: "Sales",
    status: "Registered",
    version: 1,
    rationale: null,
    config: ordersConfig("Total"),
    tableRef: null,
    warnings: [],
    ...overrides,
});

const workingChat = () => ({ ...EMPTY_CHAT, turns: [createReplyTurn(false)], abortController: new AbortController() });

/** A session whose proposal chat and Sales chat each have a turn running. */
function session(overrides: Partial<PlannerState> = {}): PlannerState {
    return {
        ...initialPlannerState,
        plannerSelectedTables: [ORDERS],
        plannerDefaultSchema: "dbo",
        plannerProposalChat: workingChat(),
        plannerAreaChats: { Sales: workingChat() },
        ...overrides,
    };
}

function fold(state: PlannerState, chatId: PlannerChatId, ...frames: MigrationFrame[]): PlannerState {
    return frames.reduce((current, frame) => ({ ...current, ...applyPlannerFrame(current, chatId, frame) }), state);
}

function lastTurn(state: PlannerState, chatId: PlannerChatId): PlannerReplyTurn {
    const turn = selectChat(state, chatId).turns.at(-1);

    if (turn?.kind !== "planner") {
        throw new Error("Expected a reply turn");
    }

    return turn;
}

describe("applyPlannerFrame", () => {
    it("keeps the proposal, with its tables resolved, and counts what it proposes", () => {
        const state = fold(session(), null, {
            type: "proposal",
            proposal: {
                areas: [
                    {
                        area: "Sales",
                        why: null,
                        collections: [{ collection: "Orders", rootTable: "dbo.orders", absorbs: [], why: null }],
                    },
                ],
                dropped: [],
                enables: [],
            },
        });

        expect(state.plannerProposal?.areas[0].collections[0].rootTableRef).toEqual(ORDERS);
        expect(lastTurn(state, null).activity).toEqual([{ type: "proposal" }]);
    });

    it("files what an area agent registers under that area, in that area's chat only", () => {
        const state = fold(session(), "Sales", {
            type: "collection",
            status: "registered",
            collection: "Orders",
            version: 1,
            config: ordersConfig("Total"),
            warnings: [],
        });

        expect(state.plannerEntries.Orders).toMatchObject({
            area: "Sales",
            status: "Registered",
            tableRef: { table: ORDERS },
        });
        expect(lastTurn(state, "Sales").activity).toEqual([
            { type: "registered", collection: "Orders", version: 1, warningCount: 0 },
        ]);
        expect(lastTurn(state, null).activity).toEqual([]);
    });

    it("shows a failed attempt the agent retries lightly, and forgets it once the collection registers", () => {
        const retrying = fold(session(), "Sales", {
            type: "transientCollectionError",
            collection: "Orders",
            errors: [{ message: "Price is mapped twice." }],
        });

        expect(retrying.plannerRetrying.Orders).toEqual(["Price is mapped twice."]);
        expect(retrying.plannerEntries.Orders).toBeUndefined();

        const registered = fold(retrying, "Sales", {
            type: "collection",
            status: "registered",
            collection: "Orders",
            version: 1,
            config: ordersConfig("Total"),
            warnings: [],
        });

        expect(registered.plannerRetrying.Orders).toBeUndefined();
    });

    it("keeps a rejected collection with the agent's last attempt and where it fails", () => {
        const errors = [{ message: "Primary key column OrderId does not exist.", path: "primaryKeyColumns[0]" }];
        const state = fold(session(), "Sales", {
            type: "rejectedCollection",
            collection: "Orders",
            config: ordersConfig("Total"),
            errors,
        });

        expect(state.plannerEntries.Orders).toMatchObject({ area: "Sales", status: "Rejected" });
        expect(state.plannerRejectionIssues.Orders).toEqual(errors);
        expect(lastTurn(state, "Sales").activity).toEqual([
            { type: "rejected", collection: "Orders", errorCount: 1, isChange: false },
        ]);
    });

    it("leaves a registered collection as it was when a change to it is rejected", () => {
        const state = fold(session({ plannerEntries: { Orders: ordersEntry() } }), "Sales", {
            type: "rejectedCollection",
            collection: "Orders",
            config: ordersConfig("Freight"),
            errors: [{ message: "Freight is not a column." }],
        });

        expect(state.plannerEntries.Orders).toEqual(ordersEntry());
        expect(lastTurn(state, "Sales").activity).toEqual([
            { type: "rejected", collection: "Orders", errorCount: 1, isChange: true },
        ]);
    });

    it("marks a collection the agent removed, with its reason", () => {
        const state = fold(session({ plannerEntries: { Orders: ordersEntry() } }), "Sales", {
            type: "removed",
            collection: "Orders",
            reason: "Merged into Invoices.",
        });

        expect(state.plannerEntries.Orders.status).toBe("Removed");
        expect(lastTurn(state, "Sales").activity).toEqual([
            { type: "removed", collection: "Orders", reason: "Merged into Invoices." },
        ]);
    });

    it("keeps the conventions the proposal agent set", () => {
        const state = fold(session(), null, {
            type: "conventions",
            propertyCase: "CamelCase",
            propertyLanguage: "Spanish",
        });

        expect(state.plannerConventions).toEqual({
            propertyCase: "CamelCase",
            propertyLanguage: "Spanish",
            notes: null,
        });
    });

    it("takes the reply with its gaps and questions, and drops a recommendation that points at no option", () => {
        const state = fold(session(), null, {
            type: "reply",
            reply: "  **Done.**  ",
            gaps: ["No default address."],
            openQuestions: [{ question: "Casing?", options: ["camelCase"], recommended: 4 }],
        });

        expect(lastTurn(state, null)).toMatchObject({
            reply: "**Done.**",
            gaps: ["No default address."],
            questions: [{ question: "Casing?", options: ["camelCase"], recommended: null }],
        });
    });

    it("ends a stopped turn as stopped, and any other error as a failure", () => {
        expect(lastTurn(fold(session(), null, { type: "error", message: "The turn was stopped." }), null).status).toBe(
            "stopped",
        );
        expect(
            lastTurn(fold(session(), null, { type: "error", message: "The AI service could not be reached." }), null),
        ).toMatchObject({ status: "failed", error: "The AI service could not be reached." });
    });

    it("keeps a finished turn finished when the stream fails after it", () => {
        const state = fold(
            session(),
            null,
            { type: "done", conversationId: "c/1", planId: "p/1" },
            { type: "error", message: "The AI service stopped answering before the turn finished." },
        );

        expect(lastTurn(state, null).status).toBe("done");
        expect(lastTurn(state, null).activity.at(-1)?.type).toBe("lateError");
    });
});

describe("endReplyTurn", () => {
    it("fails a turn whose stream ended before the agent said it was done", () => {
        const [turn] = endReplyTurn([createReplyTurn(false)], { error: null, isStopped: false });

        expect(turn).toMatchObject({ status: "failed" });
    });

    it("marks a disconnected stream as stopped", () => {
        const [turn] = endReplyTurn([createReplyTurn(false)], { error: null, isStopped: true });

        expect(turn).toMatchObject({ status: "stopped" });
    });
});

describe("endPlannerTurn", () => {
    it("stops showing the collections the turn was retrying, however it ended", () => {
        const retrying = fold(session(), "Sales", {
            type: "transientCollectionError",
            collection: "Orders",
            errors: [{ message: "Total is not a column." }],
        });
        const state = { ...retrying, plannerRetrying: { ...retrying.plannerRetrying, Products: ["Elsewhere."] } };

        const ended = { ...state, ...endPlannerTurn(state, "Sales", { error: null, isStopped: true }) };

        expect(ended.plannerRetrying).toEqual({ Products: ["Elsewhere."] });
        expect(lastTurn(ended, "Sales").status).toBe("stopped");
        expect(selectChat(ended, "Sales").abortController).toBeNull();
    });
});

describe("applyStoredPlan", () => {
    const stored = (entries: MigrationPlanGetResponse["plan"]["entries"]): MigrationPlanGetResponse => ({
        plan: { id: "p/1", slug: "shop", conventions: { propertyCase: "PascalCase" }, entries },
        proposal: null,
    });

    it("keys every entry by its name, which is the entry's identity", () => {
        const update = applyStoredPlan(
            session(),
            stored({ Clients: { area: "Sales", config: ordersConfig("Total"), version: 2, status: "Registered" } }),
        );

        expect(update.plannerEntries?.Clients).toMatchObject({ name: "Clients", area: "Sales", version: 2 });
        expect(update.plannerConventions).toEqual({ propertyCase: "PascalCase" });
    });

    it("keeps what a turn still streaming registered since the plan was read", () => {
        const state = session({ plannerEntries: { Orders: ordersEntry({ version: 2 }) } });

        const update = applyStoredPlan(state, stored({ Orders: { area: "Sales", version: 1, status: "Registered" } }));

        expect(update.plannerEntries?.Orders.version).toBe(2);
    });

    it("takes the stored plan over what a finished turn left", () => {
        const state = session({
            plannerAreaChats: {},
            plannerEntries: { Orders: ordersEntry({ status: "Rejected" }) },
            plannerRejectionIssues: { Orders: [{ message: "Bad key." }] },
        });

        const update = applyStoredPlan(state, stored({}));

        expect(update.plannerEntries).toEqual({});
        expect(update.plannerRejectionIssues).toEqual({});
    });
});

describe("isAreaNotStartedError", () => {
    it("recognizes the answer for an area that never started", () => {
        expect(isAreaNotStartedError("The area Sales has not started; start it first.")).toBe(true);
        expect(isAreaNotStartedError("The turn was stopped.")).toBe(false);
    });
});
