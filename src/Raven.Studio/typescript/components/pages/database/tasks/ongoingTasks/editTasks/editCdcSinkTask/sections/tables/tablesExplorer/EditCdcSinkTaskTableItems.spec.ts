import {
    ExplorerRowRootTable,
    FormEmbeddedTable,
    FormRootTable,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskTypes";
import { buildExplorerRows } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/sections/tables/tablesExplorer/EditCdcSinkTaskTableItems";

describe("buildExplorerRows", () => {
    it("keeps distinct root paths when the same table name exists in multiple schemas", () => {
        const rows = buildExplorerRows({
            allTables: [
                createRootTable({ sourceTableSchema: "dbo", sourceTableName: "orders", collectionName: "Orders" }),
                createRootTable({
                    sourceTableSchema: "sales",
                    sourceTableName: "orders",
                    collectionName: "SalesOrders",
                }),
            ],
            expandedTables: {},
            filter: "",
            rootFieldIds: ["root-1", "root-2"],
        });

        const rootRows = getRootRows(rows);

        expect(rootRows.map((row) => row.path)).toEqual(["tables.0", "tables.1"]);
        expect(rootRows.map((row) => row.rowKey)).toEqual(["root:root-1", "root:root-2"]);
    });

    it("keeps the surviving root row key stable after removing an earlier root row", () => {
        const tables = [
            createRootTable({ sourceTableSchema: "dbo", sourceTableName: "orders", collectionName: "Orders" }),
            createRootTable({ sourceTableSchema: "sales", sourceTableName: "orders", collectionName: "SalesOrders" }),
        ];

        const beforeRows = getRootRows(
            buildExplorerRows({
                allTables: tables,
                expandedTables: {},
                filter: "",
                rootFieldIds: ["root-1", "root-2"],
            })
        );

        const afterRows = getRootRows(
            buildExplorerRows({
                allTables: tables.slice(1),
                expandedTables: {},
                filter: "",
                rootFieldIds: ["root-2"],
            })
        );

        expect(beforeRows[1].rowKey).toBe("root:root-2");
        expect(afterRows[0].path).toBe("tables.0");
        expect(afterRows[0].rowKey).toBe(beforeRows[1].rowKey);
    });

    it("adds linked-table warnings to explorer rows", () => {
        const rows = buildExplorerRows({
            allTables: [
                createRootTable({
                    linkedTables: [
                        {
                            sourceTableName: "companies",
                            sourceTableSchema: "dbo",
                            linkedCollectionName: "Companies",
                            propertyName: "Company",
                            joinColumns: [{ value: "CompanyId" }],
                        },
                    ],
                }),
            ],
            expandedTables: {
                "tables.0": true,
            },
            filter: "",
            rootFieldIds: ["root-1"],
        });

        const rootRow = getRootRows(rows)[0];
        const linkedRow = rows.find((row) => row.type === "linked");

        expect(rootRow.warningMessages).toEqual([expect.stringContaining('"Companies" collection')]);
        expect(linkedRow?.warningMessages).toEqual([expect.stringContaining('"Companies" collection')]);
    });

    it("adds linked-table warnings to collapsed root explorer rows", () => {
        const rows = buildExplorerRows({
            allTables: [
                createRootTable({
                    linkedTables: [
                        {
                            sourceTableName: "media",
                            sourceTableSchema: "public",
                            linkedCollectionName: "Media",
                            propertyName: "Media",
                            joinColumns: [{ value: "MediaId" }],
                        },
                    ],
                }),
            ],
            expandedTables: {},
            filter: "",
            rootFieldIds: ["root-1"],
        });

        expect(rows.some((row) => row.type === "linked")).toBe(false);
        expect(getRootRows(rows)[0].warningMessages).toEqual([
            expect.stringContaining('"public.media" is also configured as a root table'),
        ]);
    });

    it("adds embedded-table warnings to explorer rows", () => {
        const rows = buildExplorerRows({
            allTables: [
                createRootTable({
                    sourceTableSchema: "dbo",
                    sourceTableName: "companies",
                    collectionName: "Companies",
                }),
                createRootTable({
                    embeddedTables: [
                        createEmbeddedTable({
                            propertyName: "Company",
                            sourceTableName: "companies",
                            sourceTableSchema: "dbo",
                        }),
                    ],
                }),
            ],
            expandedTables: {
                "tables.1": true,
            },
            filter: "",
            rootFieldIds: ["root-1", "root-2"],
        });

        const embeddedRow = rows.find((row) => row.type === "embedded");

        expect(getRootRows(rows)[1].warningMessages).toEqual([
            expect.stringContaining("already configured as a root table"),
        ]);
        expect(embeddedRow?.warningMessages).toEqual([expect.stringContaining("already configured as a root table")]);
    });

    it("adds descendant warnings to collapsed embedded explorer rows", () => {
        const rows = buildExplorerRows({
            allTables: [
                createRootTable({
                    embeddedTables: [
                        createEmbeddedTable({
                            linkedTables: [
                                {
                                    sourceTableName: "media",
                                    sourceTableSchema: "public",
                                    linkedCollectionName: "Media",
                                    propertyName: "Media",
                                    joinColumns: [{ value: "MediaId" }],
                                },
                            ],
                        }),
                    ],
                }),
            ],
            expandedTables: {
                "tables.0": true,
            },
            filter: "",
            rootFieldIds: ["root-1"],
        });

        const embeddedRow = rows.find((row) => row.type === "embedded");

        expect(rows.some((row) => row.type === "linked")).toBe(false);
        expect(embeddedRow?.warningMessages).toEqual([
            expect.stringContaining('"public.media" is also configured as a root table'),
        ]);
    });
});

function getRootRows(rows: ReturnType<typeof buildExplorerRows>) {
    return rows.filter((row): row is ExplorerRowRootTable => row.type === "root");
}

function createRootTable(overrides: Partial<FormRootTable>): FormRootTable {
    return {
        collectionName: "Orders",
        columns: [
            {
                column: "Id",
                name: "Id",
                type: "Default",
            },
        ],
        disabled: false,
        embeddedTables: [],
        linkedTables: [],
        onDelete: {
            ignoreDeletes: false,
            patch: "",
        },
        patch: "",
        primaryKeyColumns: [{ value: "Id" }],
        sourceTableName: "orders",
        sourceTableSchema: "dbo",
        ...overrides,
    };
}

function createEmbeddedTable(overrides: Partial<FormRootTable["embeddedTables"][number]>): FormEmbeddedTable {
    return {
        caseSensitiveKeys: false,
        columns: [
            {
                column: "Id",
                name: "Id",
                type: "Default",
            },
        ],
        embeddedTables: [],
        joinColumns: [{ value: "CompanyId" }],
        linkedTables: [],
        onDelete: {
            ignoreDeletes: false,
            patch: "",
        },
        patch: "",
        primaryKeyColumns: [{ value: "Id" }],
        propertyName: "Company",
        sourceTableName: "companies",
        sourceTableSchema: "dbo",
        type: "Array",
        ...overrides,
    };
}
