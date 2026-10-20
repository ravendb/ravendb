import { describe, expect, it } from "vitest";
import type { CdcSinkTableConfig } from "@/api/generated/server-api";
import {
    resolveConfigTables,
    resolveProposalTables,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-table-refs";
import type { SelectedTables } from "@/pages/setup/add-app-wizard/steps/map/session/planner-types";

const ORDERS = { sourceTableSchema: "public", sourceTableName: "orders" };

const CUSTOMERS = { sourceTableSchema: "public", sourceTableName: "customers" };

function resolveName(name: string, selectedTables: SelectedTables) {
    const { collections } = resolveProposalTables(
        {
            areas: [],
            collections: [{ collection: "Collection", rootTable: name, absorbs: [], why: null, fields: [] }],
            dropped: [],
            enables: [],
        },
        selectedTables,
    );

    return collections[0].rootTableRef;
}

describe("resolveProposalTables", () => {
    it("resolves the tables the proposal names among the selected ones, ignoring case", () => {
        const proposal = resolveProposalTables(
            {
                areas: [],
                collections: [
                    {
                        collection: "Orders",
                        rootTable: "PUBLIC.Orders",
                        absorbs: [
                            { table: "customers", how: "Link", why: null },
                            { table: "public.audit_log", how: "Embed", why: null },
                        ],
                        why: null,
                        fields: [],
                    },
                ],
                dropped: [{ table: "no_such_table", why: null }],
                enables: [],
            },
            [ORDERS, CUSTOMERS],
        );

        const [orders] = proposal.collections;
        expect(orders.rootTableRef).toEqual(ORDERS);
        expect(orders.absorbs.map((absorbed) => absorbed.tableRef)).toEqual([CUSTOMERS, null]);
        expect(proposal.dropped[0].tableRef).toBeNull();
    });

    it("leaves a bare table name two schemas share unresolved, and lets the exact spelling tell apart names that differ in case", () => {
        const selectedTables = [
            { sourceTableSchema: "sales", sourceTableName: "Orders" },
            { sourceTableSchema: "archive", sourceTableName: "Orders" },
            { sourceTableSchema: "public", sourceTableName: "Items" },
            { sourceTableSchema: "public", sourceTableName: "items" },
        ];

        expect(resolveName("Orders", selectedTables)).toBeNull();
        expect(resolveName("Archive.orders", selectedTables)).toEqual(selectedTables[1]);
        expect(resolveName("items", selectedTables)).toEqual(selectedTables[3]);
        expect(resolveName("ITEMS", selectedTables)).toBeNull();
    });
});

describe("resolveConfigTables", () => {
    it("resolves every table a configuration reads, an embedded table without a schema in its parent's", () => {
        const config: CdcSinkTableConfig = {
            collectionName: "Orders",
            sourceTableSchema: "public",
            sourceTableName: "ORDERS",
            embeddedTables: [
                { sourceTableName: "customers", propertyName: "Customer" },
                { sourceTableSchema: "public", sourceTableName: "no_such_table", propertyName: "Missing" },
            ],
        };

        expect(resolveConfigTables(config, [ORDERS, CUSTOMERS])).toEqual({
            table: ORDERS,
            embeddedTables: [
                { table: CUSTOMERS, embeddedTables: [] },
                { table: null, embeddedTables: [] },
            ],
        });
    });
});
