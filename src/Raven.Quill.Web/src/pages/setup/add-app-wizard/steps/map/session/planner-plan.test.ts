import { describe, expect, it } from "vitest";
import type { CdcSinkTableConfig, DiscoverResponse } from "@/api/generated/server-api";
import { discoveredTable } from "@/mocks/discovery-fixtures";
import { droppedTable, proposedCollection } from "@/mocks/migration-mocks";
import { indexDiscovery } from "@/pages/setup/add-app-wizard/steps/map/session/planner-document";
import {
    buildPlanCollections,
    buildPlanTables,
    countChangedSince,
    countUnbuilt,
    findCollectionsLinkingTo,
    getPlanRevisions,
    groupByArea,
    createNameMatcher,
    toSearchKey,
    type PlanSource,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-plan";
import { resolveProposalTables } from "@/pages/setup/add-app-wizard/steps/map/session/planner-table-refs";
import type {
    ConfigTableRef,
    PlannerCollection,
    RegisteredCollection,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-types";

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

const selectedTables = ["Customers", "Orders", "OrderLines", "AuditLog"].map((name) => ({
    sourceTableSchema: "dbo",
    sourceTableName: name,
}));

const proposal = resolveProposalTables(
    {
        areas: [
            { area: "Sales area", collections: ["Orders"], why: null },
            { area: "Customers area", collections: ["Customers"], why: null },
        ],
        collections: [
            proposedCollection("Customers", "dbo.Customers", [], "One per customer."),
            proposedCollection(
                "Orders",
                "dbo.Orders",
                [
                    { table: "dbo.OrderLines", how: "Embed" },
                    { table: "dbo.Customers", how: "Link" },
                ],
                "An order with its lines.",
            ),
        ],
        dropped: [droppedTable("dbo.AuditLog", "Nothing in the app reads it.")],
        enables: [],
    },
    selectedTables,
);

const registered = (
    name: string,
    version: number,
    config: CdcSinkTableConfig | null = null,
    tableRef: ConfigTableRef | null = null,
): RegisteredCollection => ({
    name,
    outcome: "registered",
    version,
    registrationNumber: version,
    rationale: `${name} as registered.`,
    config,
    tableRef,
    previousConfig: null,
    warnings: [],
    rejectionErrors: [],
});

function source(collections: Record<string, PlannerCollection>, removed: string[] = []): PlanSource {
    return { plannerProposal: proposal, plannerCollections: collections, plannerRemovedCollections: removed };
}

describe("buildPlanCollections", () => {
    it("lists the proposal in its order, then what the planner registered without proposing it", () => {
        const collections = buildPlanCollections(
            source({
                Orders: registered("Orders", 2),
                Customers: {
                    name: "Customers",
                    outcome: "rejected",
                    errors: ["Id is mapped twice."],
                    config: null,
                    issues: [],
                },
                Products: registered("Products", 1),
            }),
            index,
        );

        expect(collections.map(({ name, state, area }) => ({ name, state, area }))).toEqual([
            { name: "Customers", state: "failed", area: "Customers area" },
            { name: "Orders", state: "updated", area: "Sales area" },
            { name: "Products", state: "built", area: null },
        ]);
    });

    it("explains a suggestion with the proposal and a built collection with what the planner registered", () => {
        const [customers, orders] = buildPlanCollections(source({ Orders: registered("Orders", 1) }), index);

        expect(customers).toMatchObject({ state: "suggested", why: "One per customer." });
        expect(orders).toMatchObject({ state: "built", why: "Orders as registered." });
    });

    it("leaves out a proposed collection that was taken out of the plan", () => {
        const collections = buildPlanCollections(source({}, ["Customers"]), index);

        expect(collections.map((collection) => collection.name)).toEqual(["Orders"]);
    });

    it("keeps a built collection built when only a change to it was rejected", () => {
        const [, orders] = buildPlanCollections(
            source({ Orders: { ...registered("Orders", 1), rejectionErrors: ["Total is mapped twice."] } }),
            index,
        );

        expect(orders).toMatchObject({ state: "built", errors: ["Total is mapped twice."] });
    });

    it("hands back the same collection while nothing it is made of changed", () => {
        const orders = registered("Orders", 1);
        const before = buildPlanCollections(source({ Orders: orders }), index);
        const after = buildPlanCollections(source({ Orders: orders, Products: registered("Products", 1) }), index);

        expect(after[0]).toBe(before[0]);
        expect(after[1]).toBe(before[1]);
    });
});

describe("groupByArea", () => {
    it("follows the proposal's areas and puts collections outside any area last", () => {
        const collections = buildPlanCollections(source({ Products: registered("Products", 1) }), index);

        expect(
            groupByArea(collections, proposal).map((area) => ({
                name: area.name,
                collections: area.collections.map((collection) => collection.name),
            })),
        ).toEqual([
            { name: "Sales area", collections: ["Orders"] },
            { name: "Customers area", collections: ["Customers"] },
            { name: null, collections: ["Products"] },
        ]);
    });

    it("places a collection whose area has no name outside any area, rather than nowhere", () => {
        const unnamed = { ...proposal, areas: [{ area: "", collections: ["Orders", "Customers"], why: null }] };
        const collections = buildPlanCollections({ ...source({}), plannerProposal: unnamed }, index);

        expect(groupByArea(collections, unnamed)).toEqual([{ name: null, collections }]);
    });
});

describe("buildPlanTables", () => {
    it("says where every table in the database goes", () => {
        const collections = buildPlanCollections(source({}), index);

        expect(
            buildPlanTables(index, selectedTables, collections, proposal).map(({ label, group, detail }) => ({
                label,
                group,
                detail,
            })),
        ).toEqual([
            { label: "dbo.Archive", group: "notSelected", detail: "Not selected in Verify your schema." },
            { label: "dbo.AuditLog", group: "notMigrated", detail: "Nothing in the app reads it." },
            { label: "dbo.Customers", group: "inCollections", detail: "One Customers document per row" },
            { label: "dbo.OrderLines", group: "inCollections", detail: "Inside Orders as OrderLines" },
            { label: "dbo.Orders", group: "inCollections", detail: "One Orders document per row" },
        ]);
    });

    describe("a registered configuration", () => {
        const ordersConfig: CdcSinkTableConfig = {
            collectionName: "Orders",
            sourceTableSchema: "dbo",
            sourceTableName: "orders",
            columns: [],
            embeddedTables: [{ sourceTableName: "OrderLines", propertyName: "lines", joinColumns: ["OrderId"] }],
        };

        const tablesOfOrders = (tableRef: ConfigTableRef) => {
            const collections = buildPlanCollections(
                source({ Orders: registered("Orders", 1, ordersConfig, tableRef) }),
                index,
            );

            return buildPlanTables(index, selectedTables, collections, proposal)
                .filter((table) => table.collection?.name === "Orders")
                .map(({ label, detail }) => ({ label, detail }));
        };

        it("places its tables where they resolved to", () => {
            expect(
                tablesOfOrders({
                    table: { sourceTableSchema: "dbo", sourceTableName: "Orders" },
                    embeddedTables: [
                        { table: { sourceTableSchema: "dbo", sourceTableName: "OrderLines" }, embeddedTables: [] },
                    ],
                }),
            ).toEqual([
                { label: "dbo.OrderLines", detail: "Inside Orders as lines" },
                { label: "dbo.Orders", detail: "One Orders document per row" },
            ]);
        });

        it("places no table that fits no selected table", () => {
            expect(tablesOfOrders({ table: null, embeddedTables: [{ table: null, embeddedTables: [] }] })).toEqual([]);
        });
    });
});

describe("countUnbuilt", () => {
    it("counts suggested and failed collections, which applying leaves out", () => {
        expect(countUnbuilt(source({}))).toBe(2);
        expect(
            countUnbuilt(
                source({
                    Orders: registered("Orders", 1),
                    Customers: { name: "Customers", outcome: "rejected", errors: [], config: null, issues: [] },
                }),
            ),
        ).toBe(1);
        expect(countUnbuilt(source({ Orders: registered("Orders", 1) }, ["Customers"]))).toBe(0);
    });
});

describe("findCollectionsLinkingTo", () => {
    it("finds the collections that link to the target, ignoring case", () => {
        const collections = buildPlanCollections(source({}), index);

        expect(findCollectionsLinkingTo(collections, "customers")).toEqual(["Orders"]);
        expect(findCollectionsLinkingTo(collections, "Orders")).toEqual([]);
    });
});

describe("countChangedSince", () => {
    it("counts the collections that were built, updated or added since the panel was closed", () => {
        const before = source({});
        const after = source({ Orders: registered("Orders", 1), Products: registered("Products", 1) });

        expect(countChangedSince(getPlanRevisions(before), after)).toBe(2);
        expect(countChangedSince(getPlanRevisions(after), after)).toBe(0);
    });

    it("counts a collection registered again after a removal, although its version started over", () => {
        const first = source({ Orders: { ...registered("Orders", 1), registrationNumber: 1 } });
        const again = source({ Orders: { ...registered("Orders", 1), registrationNumber: 3 } });

        expect(countChangedSince(getPlanRevisions(first), again)).toBe(1);
    });
});

describe("createNameMatcher", () => {
    it("matches any name containing the filter, ignoring case and surrounding spaces", () => {
        expect(createNameMatcher(" orderl ")(toSearchKey("Orders", "dbo.OrderLines"))).toBe(true);
        expect(createNameMatcher("order")(toSearchKey("Customers"))).toBe(false);
        expect(createNameMatcher("  ")(toSearchKey("Customers"))).toBe(true);
    });
});
