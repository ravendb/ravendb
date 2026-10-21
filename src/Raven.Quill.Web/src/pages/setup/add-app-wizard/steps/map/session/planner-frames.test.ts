import { describe, expect, it } from "vitest";
import type { CdcSinkTableConfig, MigrationFrame } from "@/api/generated/server-api";
import {
    applyPlannerFrame,
    createReplyTurn,
    endReplyTurn,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-frames";
import { initialPlannerState, type PlannerState } from "@/pages/setup/add-app-wizard/steps/map/session/planner-state";
import type {
    PlannerReplyTurn,
    RegisteredCollection,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-types";

const ordersConfig = (column: string): CdcSinkTableConfig => ({
    collectionName: "Orders",
    sourceTableSchema: "dbo",
    sourceTableName: "Orders",
    columns: [{ column, name: column.toLowerCase() }],
});

const builtOrders: RegisteredCollection = {
    name: "Orders",
    outcome: "registered",
    version: 1,
    registrationNumber: 1,
    rationale: null,
    config: ordersConfig("Total"),
    tableRef: null,
    previousConfig: null,
    warnings: [],
    rejectionErrors: [],
};

function stateWithTurn(overrides: Partial<PlannerState> = {}): PlannerState {
    return { ...initialPlannerState, plannerTurns: [createReplyTurn(true)], ...overrides };
}

function fold(state: PlannerState, ...frames: MigrationFrame[]): PlannerState {
    return frames.reduce((current, frame) => ({ ...current, ...applyPlannerFrame(current, frame) }), state);
}

function lastTurn(state: Partial<PlannerState>): PlannerReplyTurn {
    const turn = state.plannerTurns?.at(-1);

    if (turn?.kind !== "planner") {
        throw new Error("Expected a reply turn");
    }

    return turn;
}

describe("applyPlannerFrame", () => {
    it("keeps the proposal and notes which collections it lists", () => {
        const update = applyPlannerFrame(stateWithTurn(), {
            type: "proposal",
            areas: [{ area: "Sales", collections: ["Orders"], why: null }],
            collections: [
                { collection: "Orders", rootTable: "dbo.Orders", absorbs: [], why: "One per order.", fields: [] },
            ],
            dropped: [{ table: "dbo.AuditLog", why: "Nothing reads it." }],
            enables: [],
        });

        expect(update.plannerProposal?.collections).toHaveLength(1);
        expect(lastTurn(update).activity).toEqual([
            { type: "proposal", collections: ["Orders"], areaCount: 1, droppedCount: 1 },
        ]);
    });

    it("resolves the tables a proposal and a registered configuration name among the selected tables", () => {
        const orders = { sourceTableSchema: "dbo", sourceTableName: "Orders" };
        const state = stateWithTurn({ plannerSelectedTables: [orders] });

        const proposed = applyPlannerFrame(state, {
            type: "proposal",
            areas: [],
            collections: [{ collection: "Orders", rootTable: "orders", absorbs: [], why: null, fields: [] }],
            dropped: [{ table: "dbo.AuditLog", why: "Nothing reads it." }],
            enables: [],
        });
        const registered = applyPlannerFrame(state, {
            type: "collection",
            status: "registered",
            collection: "Orders",
            version: 1,
            config: ordersConfig("Total"),
            warnings: [],
        });

        expect(proposed.plannerProposal?.collections[0].rootTableRef).toEqual(orders);
        expect(proposed.plannerProposal?.dropped[0].tableRef).toBeNull();
        expect(registered.plannerCollections?.Orders).toMatchObject({
            tableRef: { table: orders, embeddedTables: [] },
        });
    });

    it("keeps the configuration a re-registration replaced, so the details can mark what is new", () => {
        const first = applyPlannerFrame(stateWithTurn(), {
            type: "collection",
            status: "registered",
            collection: "Orders",
            version: 1,
            config: ordersConfig("Total"),
            warnings: [],
        });
        const second = applyPlannerFrame(stateWithTurn({ plannerCollections: first.plannerCollections }), {
            type: "collection",
            status: "replaced",
            collection: "Orders",
            version: 2,
            config: ordersConfig("PlacedAt"),
            warnings: ["Total can lose precision."],
        });

        expect(second.plannerCollections?.Orders).toMatchObject({
            outcome: "registered",
            version: 2,
            config: ordersConfig("PlacedAt"),
            previousConfig: ordersConfig("Total"),
        });
        expect(lastTurn(second).activity).toEqual([
            { type: "registered", collection: "Orders", version: 2, warningCount: 1 },
        ]);
    });

    it("keeps a rejected collection, with why it was rejected and the mapping the planner generated", () => {
        const update = applyPlannerFrame(stateWithTurn(), {
            type: "rejectedCollection",
            collection: "Products",
            config: ordersConfig("Price"),
            errors: [
                { message: "Price is mapped twice.", path: "columns[1].name", property: "Price" },
                { message: "ProductId does not exist." },
            ],
        });

        expect(update.plannerCollections?.Products).toEqual({
            name: "Products",
            outcome: "rejected",
            errors: ["Price is mapped twice.", "ProductId does not exist."],
            config: ordersConfig("Price"),
            issues: [
                { message: "Price is mapped twice.", path: "columns[1].name", property: "Price" },
                { message: "ProductId does not exist." },
            ],
        });
        expect(lastTurn(update).activity).toEqual([
            { type: "rejected", collection: "Products", errorCount: 2, isChange: false },
        ]);
    });

    it("keeps a built collection whose change was rejected, and says it was only the change", () => {
        const update = applyPlannerFrame(stateWithTurn({ plannerCollections: { Orders: builtOrders } }), {
            type: "rejectedCollection",
            collection: "Orders",
            config: ordersConfig("Total"),
            errors: [{ message: "Total is mapped twice." }],
        });

        expect(update.plannerCollections?.Orders).toMatchObject({
            outcome: "registered",
            version: 1,
            config: ordersConfig("Total"),
            rejectionErrors: ["Total is mapped twice."],
        });
        expect(lastTurn(update).activity).toEqual([
            { type: "rejected", collection: "Orders", errorCount: 1, isChange: true },
        ]);
    });

    it("shows a transient error as activity only, leaving the collection as it was", () => {
        const update = applyPlannerFrame(stateWithTurn({ plannerCollections: { Orders: builtOrders } }), {
            type: "transientCollectionError",
            collection: "Orders",
            errors: [{ message: "Total is mapped twice.", path: "columns[1].name" }],
            rejections: 1,
            maxRejections: 2,
        });

        expect(update.plannerCollections).toBeUndefined();
        expect(lastTurn(update).activity).toEqual([
            {
                type: "transientError",
                collection: "Orders",
                errors: ["Total is mapped twice."],
                rejections: 1,
                maxRejections: 2,
            },
        ]);
    });

    it("numbers every registration, so a collection registered again after a removal is a new one", () => {
        const registeredOrders: MigrationFrame = {
            type: "collection",
            status: "registered",
            collection: "Orders",
            version: 1,
            config: ordersConfig("Total"),
            warnings: [],
        };

        const first = fold(stateWithTurn(), registeredOrders);
        const again = fold(first, { type: "removed", collection: "Orders", reason: null }, registeredOrders);

        expect(first.plannerCollections.Orders).toMatchObject({ version: 1, registrationNumber: 1 });
        expect(again.plannerCollections.Orders).toMatchObject({ version: 1, registrationNumber: 2 });
    });

    it("drops a removed collection, and remembers it so the proposal does not bring it back", () => {
        const state = stateWithTurn({
            plannerCollections: {
                Products: { name: "Products", outcome: "rejected", errors: [], config: null, issues: [] },
            },
        });

        const update = applyPlannerFrame(state, { type: "removed", collection: "Products", reason: null });

        expect(update.plannerCollections).toEqual({});
        expect(update.plannerRemovedCollections).toEqual(["Products"]);
    });

    it("forgets the mention and the open details of a collection the planner removed", () => {
        const state = stateWithTurn({
            plannerCollections: { Orders: builtOrders },
            plannerMentions: ["Orders"],
            plannerDetailsCollection: "Orders",
        });

        const update = applyPlannerFrame(state, { type: "removed", collection: "Orders", reason: null });

        expect(update.plannerMentions).toEqual([]);
        expect(update.plannerDetailsCollection).toBeNull();
    });

    it("forgets a removal once the planner registers the collection again", () => {
        const update = applyPlannerFrame(stateWithTurn({ plannerRemovedCollections: ["Orders"] }), {
            type: "collection",
            status: "registered",
            collection: "Orders",
            version: 1,
            config: ordersConfig("Total"),
            warnings: [],
        });

        expect(update.plannerRemovedCollections).toEqual([]);
    });

    it("keeps each proposed collection's field names on the collection", () => {
        const update = applyPlannerFrame(stateWithTurn(), {
            type: "proposal",
            areas: [],
            collections: [
                {
                    collection: "Orders",
                    rootTable: "dbo.Orders",
                    absorbs: [],
                    why: null,
                    fields: [{ table: "dbo.Orders", column: "order_id", property: "OrderId" }],
                },
            ],
            dropped: [],
            enables: [],
        });

        expect(update.plannerProposal?.collections[0].fields).toEqual([
            { table: "dbo.Orders", column: "order_id", property: "OrderId" },
        ]);
    });

    it("skips conventions that change nothing", () => {
        const update = applyPlannerFrame(stateWithTurn(), {
            type: "conventions",
            propertyCase: "Unspecified",
            mustReEmit: [],
        });

        expect(update).toEqual({});
    });

    it("keeps conventions that leave the casing alone but set a language or re-emit collections", () => {
        const update = applyPlannerFrame(stateWithTurn(), {
            type: "conventions",
            propertyCase: "Unspecified",
            propertyLanguage: "Spanish",
            mustReEmit: ["Orders"],
        });

        expect(lastTurn(update).activity).toEqual([
            {
                type: "conventions",
                propertyCase: "Unspecified",
                propertyLanguage: "Spanish",
                notes: null,
                mustReEmit: ["Orders"],
            },
        ]);
    });

    it("keeps the open questions on the turn that asked them, a recommendation only when it points at an option", () => {
        const update = applyPlannerFrame(stateWithTurn(), {
            type: "reply",
            reply: " Here is the plan. ",
            gaps: ["No column marks the default address."],
            openQuestions: [
                { question: "Embed lines?", options: ["Yes", "No"], recommended: 0 },
                { question: "Casing?", options: ["camelCase"], recommended: 4 },
                { question: "Anything else?", options: [], recommended: null },
            ],
        });

        expect(lastTurn(update)).toMatchObject({
            reply: "Here is the plan.",
            gaps: ["No column marks the default address."],
            questions: [
                { question: "Embed lines?", options: ["Yes", "No"], recommended: 0 },
                { question: "Casing?", options: ["camelCase"], recommended: null },
                { question: "Anything else?", options: [], recommended: null },
            ],
        });
    });

    it("fails a turn the planner did not finish", () => {
        const update = applyPlannerFrame(stateWithTurn(), { type: "error", message: "Could not store the schema." });

        expect(lastTurn(update)).toMatchObject({ status: "failed", error: "Could not store the schema." });
        expect(update).not.toHaveProperty("plannerConversationId");
    });

    it("keeps a finished turn finished when an error follows it, and notes the error", () => {
        const done = applyPlannerFrame(stateWithTurn(), { type: "done", conversationId: "planner/1" });

        const update = applyPlannerFrame(stateWithTurn({ plannerTurns: done.plannerTurns }), {
            type: "error",
            message: "The AI service could not be reached.",
        });

        expect(lastTurn(update)).toMatchObject({
            status: "done",
            error: null,
            activity: [{ type: "lateError", message: "The AI service could not be reached." }],
        });
    });

    it("opens the conversation when the turn is done", () => {
        const update = applyPlannerFrame(stateWithTurn(), { type: "done", conversationId: "planner/1" });

        expect(update.plannerConversationId).toBe("planner/1");
        expect(lastTurn(update).status).toBe("done");
    });
});

describe("endReplyTurn", () => {
    const working = stateWithTurn().plannerTurns;
    const done = applyPlannerFrame(stateWithTurn(), { type: "done", conversationId: "planner/1" }).plannerTurns ?? [];

    it("marks a turn the operator stopped as stopped", () => {
        expect(lastTurn({ plannerTurns: endReplyTurn(working, { error: null, isStopped: true }) }).status).toBe(
            "stopped",
        );
    });

    it("fails a turn whose stream ended before the planner said it was done", () => {
        expect(lastTurn({ plannerTurns: endReplyTurn(working, { error: null, isStopped: false }) })).toMatchObject({
            status: "failed",
            error: "The planner ended its turn without finishing it.",
        });
    });

    it("leaves a finished turn alone", () => {
        expect(lastTurn({ plannerTurns: endReplyTurn(done, { error: null, isStopped: false }) }).status).toBe("done");
    });

    it("keeps a finished turn finished when reading its stream fails afterwards", () => {
        expect(
            lastTurn({ plannerTurns: endReplyTurn(done, { error: "network error", isStopped: false }) }),
        ).toMatchObject({ status: "done", activity: [{ type: "lateError", message: "network error" }] });
    });
});
