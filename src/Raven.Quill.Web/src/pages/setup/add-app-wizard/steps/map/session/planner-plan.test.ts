import { describe, expect, it } from "vitest";
import type { CdcSinkTableConfig, DiscoverResponse } from "@/api/generated/server-api";
import { discoveredTable } from "@/mocks/discovery-fixtures";
import { droppedTable, proposedCollection } from "@/mocks/migration-mocks";
import { indexDiscovery } from "@/pages/setup/add-app-wizard/steps/map/session/planner-document";
import {
    buildPlan,
    createNameMatcher,
    getAreaShortName,
    getCollectionSources,
    toSearchKey,
    type PlanSource,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-plan";
import { createReplyTurn } from "@/pages/setup/add-app-wizard/steps/map/session/planner-frames";
import { EMPTY_CHAT, initialPlannerState } from "@/pages/setup/add-app-wizard/steps/map/session/planner-state";
import {
    resolveConfigTables,
    resolveProposalTables,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-table-refs";
import type { PlannerEntry } from "@/pages/setup/add-app-wizard/steps/map/session/planner-types";

const discovery: DiscoverResponse = {
    catalogName: "demo_shop",
    success: true,
    hasPermissionToSetup: true,
    errors: [],
    warnings: [],
    tables: [
        discoveredTable("dbo", "Customers", ["Id", "Name"]),
        discoveredTable("dbo", "Orders", ["Id", "CustomerId", "Total"], { CustomerId: "Customers" }),
        discoveredTable("dbo", "OrderLines", ["Id", "OrderId", "Quantity"], { OrderId: "Orders" }),
        discoveredTable("dbo", "AuditLog", ["Id", "Entry"]),
        discoveredTable("dbo", "Archive", ["Id", "Entry"]),
    ],
};

const index = indexDiscovery(discovery);

const selectedTables = ["Customers", "Orders", "OrderLines", "AuditLog", "Archive"].map((name) => ({
    sourceTableSchema: "dbo",
    sourceTableName: name,
}));

const proposal = resolveProposalTables(
    {
        areas: [
            {
                area: "Customers",
                why: "Who buys.",
                collections: [proposedCollection("Customers", "dbo.Customers", [], "One document per customer.")],
            },
            {
                area: "Sales",
                why: "What is sold.",
                collections: [
                    {
                        ...proposedCollection(
                            "Orders",
                            "dbo.Orders",
                            [
                                { table: "dbo.OrderLines", how: "Embed" },
                                { table: "dbo.Customers", how: "Link" },
                            ],
                            "An order with its lines.",
                        ),
                        fields: [{ table: "dbo.Orders", column: "Total", property: "total" }],
                    },
                ],
            },
        ],
        dropped: [droppedTable("dbo.AuditLog", "Nothing reads it.")],
        enables: [],
    },
    selectedTables,
);

const config = (collectionName: string, sourceTableName: string): CdcSinkTableConfig => ({
    collectionName,
    sourceTableSchema: "dbo",
    sourceTableName,
    columns: [{ column: "Id", name: "id" }],
});

const entry = (name: string, area: string, overrides: Partial<PlannerEntry> = {}): PlannerEntry => {
    const entryConfig = config(name, overrides.config?.sourceTableName ?? name);

    return {
        name,
        area,
        status: "Registered",
        version: 1,
        rationale: null,
        config: entryConfig,
        tableRef: resolveConfigTables(entryConfig, selectedTables, "dbo"),
        warnings: [],
        ...overrides,
    };
};

function source(overrides: Partial<PlanSource> = {}): PlanSource {
    return {
        ...initialPlannerState,
        plannerProposal: proposal,
        plannerSelectedTables: selectedTables,
        plannerDefaultSchema: "dbo",
        ...overrides,
    };
}

const stateOf = (plan: ReturnType<typeof buildPlan>, name: string) =>
    plan.collections.find((collection) => collection.name === name)?.state;

describe("buildPlan", () => {
    it("lists the proposal's areas in its order, each with its collections, nothing built yet", () => {
        const plan = buildPlan(source(), index);

        expect(plan.areas.map((area) => [area.name, area.state, area.pendingCollections])).toEqual([
            ["Customers", "notStarted", ["Customers"]],
            ["Sales", "notStarted", ["Orders"]],
        ]);
        expect(stateOf(plan, "Orders")).toBe("suggested");
    });

    it("shows the proposal's property names in the document it outlines", () => {
        const orders = buildPlan(source(), index).collections.find((collection) => collection.name === "Orders");

        expect(orders?.document?.properties.map((property) => property.name)).toEqual([
            "Id",
            "total",
            "CustomerId",
            "OrderLines",
        ]);
    });

    it("builds an area's collections while its agent works, and tells built from updated", () => {
        const building = buildPlan(
            source({
                plannerAreaChats: { Sales: { ...EMPTY_CHAT, turns: [createReplyTurn(true)] } },
                plannerStartedAreas: ["Sales"],
            }),
            index,
        );

        expect(stateOf(building, "Orders")).toBe("building");
        expect(building.areas[1].state).toBe("building");

        const built = buildPlan(
            source({
                plannerEntries: {
                    Orders: entry("Orders", "Sales", { version: 2 }),
                    Customers: entry("Customers", "Customers"),
                },
            }),
            index,
        );

        expect(stateOf(built, "Orders")).toBe("updated");
        expect(stateOf(built, "Customers")).toBe("built");
        expect(built.areas.map((area) => area.state)).toEqual(["built", "built"]);
    });

    it("shows a rejected collection as failed, with why, until an edit fixes it", () => {
        const rejected = entry("Orders", "Sales", { status: "Rejected", warnings: ["Bad key."] });
        const failed = buildPlan(source({ plannerEntries: { Orders: rejected } }), index);

        expect(failed.collections.find((collection) => collection.name === "Orders")).toMatchObject({
            state: "failed",
            problems: ["Bad key."],
        });
        expect(failed.areas[1].state).toBe("failed");

        const fixed = buildPlan(
            source({
                plannerEntries: { Orders: rejected },
                plannerEdits: {
                    Orders: { area: "Sales", config: config("Orders", "Orders"), isRemoved: false },
                },
            }),
            index,
        );

        expect(stateOf(fixed, "Orders")).toBe("built");
    });

    it("shows a moved collection only in the area it was moved to", () => {
        const plan = buildPlan(source({ plannerEntries: { Orders: entry("Orders", "Customers") } }), index);

        expect(plan.areas.map((area) => area.collections.map((collection) => collection.name))).toEqual([
            ["Customers", "Orders"],
            [],
        ]);
    });

    it("shows a renamed collection once, under its new name, and does not offer to build it again", () => {
        const plan = buildPlan(
            source({
                plannerEntries: { Purchases: entry("Purchases", "Sales", { config: config("Purchases", "Orders") }) },
            }),
            index,
        );
        const sales = plan.areas[1];

        expect(sales.collections.map((collection) => collection.name)).toEqual(["Purchases"]);
        expect(sales.pendingCollections).toEqual([]);
    });

    it("lists the selected tables no collection reads, with the planner's reason where it gave one", () => {
        const plan = buildPlan(source(), index);

        expect(plan.leftOut).toEqual([
            { table: "dbo.Archive", why: "Not in any collection.", searchKey: "dbo.archive" },
            { table: "dbo.AuditLog", why: "Nothing reads it.", searchKey: "dbo.auditlog" },
        ]);
    });
});

describe("getCollectionSources", () => {
    it("lists the table, the embedded tables and the linked collections, with no property names before a build", () => {
        const orders = buildPlan(source(), index).collections.find((collection) => collection.name === "Orders");

        expect(orders && getCollectionSources(orders)).toEqual({
            table: { kind: "table", name: "dbo.Orders", as: null },
            embeds: [{ kind: "embed", name: "dbo.OrderLines", as: null }],
            links: [{ kind: "link", name: "Customers", as: null }],
        });
    });
});

describe("getAreaShortName", () => {
    it("drops the word area from the end of the name only", () => {
        expect(getAreaShortName("Sales area")).toBe("Sales");
        expect(getAreaShortName("Customers Area")).toBe("Customers");
        expect(getAreaShortName("Area")).toBe("Area");
        expect(getAreaShortName("Staging")).toBe("Staging");
    });
});

describe("createNameMatcher", () => {
    it("matches the name or a table, ignoring case and surrounding spaces", () => {
        const matches = createNameMatcher("  orders ");

        expect(matches(toSearchKey("Sales", "dbo.Orders"))).toBe(true);
        expect(matches(toSearchKey("Customers"))).toBe(false);
        expect(createNameMatcher(" ")(toSearchKey("Customers"))).toBe(true);
    });
});
