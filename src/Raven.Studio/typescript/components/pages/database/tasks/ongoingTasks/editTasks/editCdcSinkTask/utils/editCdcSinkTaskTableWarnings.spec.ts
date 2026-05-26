import {
    FormEmbeddedTable,
    FormRootTable,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskTypes";
import {
    analyzeRootTables,
    getDuplicateRootTableErrors,
    getEmbeddedTableWarningMessagesFromAnalysis,
    getEmbeddedRootTableConflictWarning,
    getMissingRelatedCollectionWarning,
    getRootTableWarningMessagesFromAnalysis,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskTableWarnings";

describe("CDC Sink table warnings", () => {
    it("returns duplicate errors for each repeated root source table", () => {
        const duplicateErrors = getDuplicateRootTableErrors([
            createRootTable({ sourceTableSchema: "dbo", sourceTableName: "orders", collectionName: "Orders" }),
            createRootTable({ sourceTableSchema: "DBO", sourceTableName: "Orders", collectionName: "Cars" }),
            createRootTable({ sourceTableSchema: "dbo", sourceTableName: "companies", collectionName: "Companies" }),
        ]);

        expect(duplicateErrors).toEqual([
            expect.objectContaining({ index: 0, message: expect.stringContaining('"dbo.orders"') }),
            expect.objectContaining({ index: 1, message: expect.stringContaining('"DBO.Orders"') }),
        ]);
    });

    it("returns an embedded warning when the source table is already configured as a root table", () => {
        const warning = getEmbeddedRootTableConflictWarning(
            [
                createRootTable({
                    sourceTableSchema: "dbo",
                    sourceTableName: "companies",
                    collectionName: "Companies",
                }),
            ],
            {
                sourceTableName: "companies",
                sourceTableSchema: "dbo",
            }
        );

        expect(warning).toContain("already configured as a root table");
    });

    it("returns null when a matching root table creates the linked collection", () => {
        const warning = getMissingRelatedCollectionWarning(
            [
                createRootTable({
                    sourceTableSchema: "dbo",
                    sourceTableName: "companies",
                    collectionName: "companies",
                }),
            ],
            {
                linkedCollectionName: "Companies",
                propertyName: "Company",
                sourceTableName: "companies",
                sourceTableSchema: "dbo",
            }
        );

        expect(warning).toBeNull();
    });

    it("returns a warning when no root table creates the linked collection", () => {
        const warning = getMissingRelatedCollectionWarning(
            [
                createRootTable({
                    sourceTableSchema: "dbo",
                    sourceTableName: "companies",
                    collectionName: "Businesses",
                }),
            ],
            {
                linkedCollectionName: "Companies",
                propertyName: "Company",
                sourceTableName: "companies",
                sourceTableSchema: "dbo",
            }
        );

        expect(warning).toBe(`No root table is configured for the related "Companies" collection.
The Company property will contain related document IDs that reference documents in the "Companies" collection.
However, those documents will not be created unless "dbo.companies" is also configured as a root table.`);
    });

    it("returns root-level warnings from the whole table tree", () => {
        const rootTable = createRootTable({
            embeddedTables: [
                {
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
                    linkedTables: [
                        {
                            sourceTableName: "media",
                            sourceTableSchema: "public",
                            linkedCollectionName: "Media",
                            propertyName: "Media",
                            joinColumns: [{ value: "MediaId" }],
                        },
                    ],
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
                },
            ],
        });
        const conflictingRootTable = createRootTable({
            sourceTableSchema: "dbo",
            sourceTableName: "companies",
            collectionName: "Companies",
        });

        const warnings = getRootTableWarningMessagesFromAnalysis(
            analyzeRootTables([rootTable, conflictingRootTable]),
            rootTable
        );

        expect(warnings).toEqual([
            expect.stringContaining("already configured as a root table"),
            `No root table is configured for the related "Media" collection.
The Media property will contain related document IDs that reference documents in the "Media" collection.
However, those documents will not be created unless "public.media" is also configured as a root table.`,
        ]);
    });

    it("returns embedded-level warnings for the embedded table and its descendants", () => {
        const rootTable = createRootTable({
            sourceTableSchema: "dbo",
            sourceTableName: "companies",
            collectionName: "Companies",
        });
        const embeddedTable = createEmbeddedTable({
            linkedTables: [
                {
                    sourceTableName: "media",
                    sourceTableSchema: "public",
                    linkedCollectionName: "Media",
                    propertyName: "Media",
                    joinColumns: [{ value: "MediaId" }],
                },
            ],
            propertyName: "Company",
            sourceTableName: "companies",
            sourceTableSchema: "dbo",
        });

        const warnings = getEmbeddedTableWarningMessagesFromAnalysis(analyzeRootTables([rootTable]), embeddedTable);

        expect(warnings).toEqual([
            expect.stringContaining("already configured as a root table"),
            expect.stringContaining('"public.media" is also configured as a root table'),
        ]);
    });
});

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
