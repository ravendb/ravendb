import { describe, expect, it } from "vitest";
import type { CdcSinkTableConfig, DiscoverResponse } from "@/api/generated/server-api";
import { discoveredTable } from "@/mocks/discovery-fixtures";
import { proposedCollection } from "@/mocks/migration-mocks";
import {
    documentFromConfig,
    documentFromProposal,
    getEmbeddedTables,
    getLinkTargets,
    getShapeRows,
    indexDiscovery,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-document";
import { resolveProposalTables } from "@/pages/setup/add-app-wizard/steps/map/session/planner-table-refs";

const discovery: DiscoverResponse = {
    catalogName: "demo_shop",
    success: true,
    hasPermissionToSetup: true,
    errors: [],
    warnings: [],
    tables: [
        discoveredTable("dbo", "Customers", ["Id", "Name", "Email"]),
        discoveredTable("dbo", "Orders", ["Id", "CustomerId", "PlacedAt", "Total"], { CustomerId: "Customers" }),
        discoveredTable("dbo", "OrderLines", ["Id", "OrderId", "ProductId", "Quantity"], {
            OrderId: "Orders",
            ProductId: "Products",
        }),
        discoveredTable("dbo", "LineNotes", ["Id", "OrderLineId", "Text"], { OrderLineId: "OrderLines" }),
        discoveredTable("dbo", "Products", ["Id", "Name", "Price"]),
    ],
};

const index = indexDiscovery(discovery);

const { collections: proposed } = resolveProposalTables(
    {
        areas: [],
        collections: [
            proposedCollection(
                "Orders",
                "dbo.Orders",
                [
                    { table: "dbo.LineNotes", how: "Embed" },
                    { table: "dbo.OrderLines", how: "Embed" },
                    { table: "dbo.Customers", how: "Link" },
                ],
                "",
            ),
            proposedCollection("Customers", "dbo.Customers", [], ""),
        ],
        dropped: [],
        enables: [],
    },
    discovery.tables,
);

const [orders] = proposed;

const linkTargets = getLinkTargets(proposed, index);

describe("documentFromProposal", () => {
    it("reads the document from the schema, under source names, without the columns relations consume", () => {
        const document = documentFromProposal(orders, index, linkTargets);

        expect(document).toMatchObject({
            table: "dbo.Orders",
            properties: [
                { kind: "column", name: "Id" },
                { kind: "column", name: "PlacedAt" },
                { kind: "column", name: "Total", column: "Total", table: "dbo.Orders" },
                { kind: "link", name: "CustomerId", collection: "Customers", joinColumns: ["CustomerId"] },
                { kind: "embed", name: "OrderLines", relation: "Array", joinColumns: ["OrderId"] },
            ],
        });
    });

    it("nests an embedded table under the absorbed table it references, whatever order the proposal lists them in", () => {
        const document = documentFromProposal(orders, index, linkTargets);

        expect(getEmbeddedTables(document!).map(({ table, path }) => ({ table, path }))).toEqual([
            { table: "dbo.OrderLines", path: "OrderLines" },
            { table: "dbo.LineNotes", path: "OrderLines.LineNotes" },
        ]);
    });

    it("keeps a table the planner absorbed some other way inside the document", () => {
        const folded = {
            ...orders,
            absorbs: orders.absorbs.map((absorbed) =>
                absorbed.table === "dbo.OrderLines" ? { ...absorbed, how: "Fold" } : absorbed,
            ),
        };

        expect(
            getEmbeddedTables(documentFromProposal(folded, index, linkTargets)!).map((embedded) => embedded.table),
        ).toContain("dbo.OrderLines");
    });

    it("has nothing to show for a root table that fits no selected table", () => {
        expect(documentFromProposal({ ...orders, rootTableRef: null }, index, linkTargets)).toBeNull();
    });
});

describe("indexDiscovery", () => {
    it("finds a table by its exact spelling only, as a resolved table has it", () => {
        expect(index.find({ sourceTableSchema: "dbo", sourceTableName: "Orders" })?.sourceTableName).toBe("Orders");
        expect(index.find({ sourceTableSchema: "DBO", sourceTableName: "orders" })).toBeUndefined();
        expect(index.find({ sourceTableSchema: "dbo", sourceTableName: "Missing" })).toBeUndefined();
    });

    it("lists the tables by label", () => {
        expect(index.tables.map((table) => table.sourceTableName)).toEqual([
            "Customers",
            "LineNotes",
            "OrderLines",
            "Orders",
            "Products",
        ]);
    });
});

describe("getShapeRows", () => {
    const registered: CdcSinkTableConfig = {
        collectionName: "Orders",
        sourceTableSchema: "dbo",
        sourceTableName: "Orders",
        columns: [{ column: "PlacedAt", name: "placedAt" }],
        linkedTables: [{ propertyName: "customer", linkedCollectionName: "Customers", joinColumns: ["CustomerId"] }],
        embeddedTables: [
            {
                sourceTableName: "OrderLines",
                propertyName: "lines",
                type: "Array",
                joinColumns: ["OrderId"],
                columns: [{ column: "Quantity", name: "quantity" }],
                linkedTables: [
                    { propertyName: "product", linkedCollectionName: "Products", joinColumns: ["ProductId"] },
                ],
            },
        ],
    };

    it("lists every property under its parent, with its depth", () => {
        expect(
            getShapeRows(documentFromConfig(registered, null), null).map(({ path, depth }) => ({ path, depth })),
        ).toEqual([
            { path: "placedAt", depth: 0 },
            { path: "customer", depth: 0 },
            { path: "lines", depth: 0 },
            { path: "lines.quantity", depth: 1 },
            { path: "lines.product", depth: 1 },
        ]);
    });

    it("names an embedded table without a schema in its parent's, and reads it from the table it resolved to", () => {
        const orderLines = { sourceTableSchema: "dbo", sourceTableName: "OrderLines" };
        const tableRef = {
            table: { sourceTableSchema: "dbo", sourceTableName: "Orders" },
            embeddedTables: [{ table: orderLines, embeddedTables: [] }],
        };

        expect(getEmbeddedTables(documentFromConfig(registered, tableRef))).toEqual([
            { table: "dbo.OrderLines", source: orderLines, path: "lines" },
        ]);
    });

    it("reads a table that fits no selected table from nowhere", () => {
        expect(getEmbeddedTables(documentFromConfig(registered, null))).toEqual([
            { table: "dbo.OrderLines", source: null, path: "lines" },
        ]);
    });

    it("marks what the previous registration did not have", () => {
        const previous = { ...registered, embeddedTables: [{ ...registered.embeddedTables![0], linkedTables: [] }] };

        const newPaths = getShapeRows(documentFromConfig(registered, null), documentFromConfig(previous, null))
            .filter((row) => row.isNew)
            .map((row) => row.path);

        expect(newPaths).toEqual(["lines.product"]);
    });

    it("marks nothing new on a first registration", () => {
        expect(getShapeRows(documentFromConfig(registered, null), null).some((row) => row.isNew)).toBe(false);
    });
});
