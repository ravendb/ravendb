import {
    FormEmbeddedTable,
    FormRootTable,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskTypes";
import {
    analyzeRootTables,
    getDuplicateRootTableErrors,
    getEmbeddedTableWarningMessagesFromAnalysis,
    getEmbeddedRootTableConflictWarningFromAnalysis,
    getMissingRelatedCollectionWarningFromAnalysis,
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

    it("reports a duplicate even when one of the root tables is disabled", () => {
        const duplicateErrors = getDuplicateRootTableErrors([
            createRootTable({ sourceTableSchema: "dbo", sourceTableName: "orders", collectionName: "Orders" }),
            createRootTable({
                sourceTableSchema: "dbo",
                sourceTableName: "orders",
                collectionName: "Cars",
                disabled: true,
            }),
        ]);

        expect(duplicateErrors).toEqual([expect.objectContaining({ index: 0 }), expect.objectContaining({ index: 1 })]);
    });

    it("returns an embedded warning when the source table is already configured as a root table", () => {
        const warning = getEmbeddedRootTableConflictWarningFromAnalysis(
            analyzeRootTables([
                createRootTable({
                    sourceTableSchema: "dbo",
                    sourceTableName: "companies",
                    collectionName: "Companies",
                }),
            ]),
            {
                sourceTableName: "companies",
                sourceTableSchema: "dbo",
            }
        );

        expect(warning).toContain("already configured as a root table");
    });

    it("returns null when a matching root table creates the linked collection", () => {
        const warning = getMissingRelatedCollectionWarningFromAnalysis(
            analyzeRootTables([
                createRootTable({
                    sourceTableSchema: "dbo",
                    sourceTableName: "companies",
                    collectionName: "companies",
                }),
            ]),
            {
                linkedCollectionName: "Companies",
                propertyName: "Company",
                sourceTableName: "companies",
                sourceTableSchema: "dbo",
            }
        );

        expect(warning).toBeNull();
    });

    it("returns a warning when the source table is not configured as a root table", () => {
        const warning = getMissingRelatedCollectionWarningFromAnalysis(
            analyzeRootTables([
                createRootTable({
                    sourceTableSchema: "dbo",
                    sourceTableName: "orders",
                    collectionName: "Orders",
                }),
            ]),
            {
                linkedCollectionName: "Companies",
                propertyName: "Company",
                sourceTableName: "companies",
                sourceTableSchema: "dbo",
            }
        );

        expect(warning).toBe(
            `Related documents in the "Companies" collection will not be created because "dbo.companies" is not configured as a root table.`
        );
    });

    it("returns a disabled warning when the only matching root table is disabled", () => {
        const warning = getMissingRelatedCollectionWarningFromAnalysis(
            analyzeRootTables([
                createRootTable({
                    sourceTableSchema: "dbo",
                    sourceTableName: "companies",
                    collectionName: "Companies",
                    disabled: true,
                }),
            ]),
            {
                linkedCollectionName: "Companies",
                propertyName: "Company",
                sourceTableName: "companies",
                sourceTableSchema: "dbo",
            }
        );

        expect(warning).toBe(
            `Related documents in the "Companies" collection will not be created because the "dbo.companies" root table is disabled.`
        );
    });

    it("ignores the disabled duplicate when an enabled root table also matches", () => {
        const warning = getMissingRelatedCollectionWarningFromAnalysis(
            analyzeRootTables([
                createRootTable({
                    sourceTableSchema: "dbo",
                    sourceTableName: "companies",
                    collectionName: "Companies",
                }),
                createRootTable({
                    sourceTableSchema: "dbo",
                    sourceTableName: "companies",
                    collectionName: "Archive",
                    disabled: true,
                }),
            ]),
            {
                linkedCollectionName: "Companies",
                propertyName: "Company",
                sourceTableName: "companies",
                sourceTableSchema: "dbo",
            }
        );

        expect(warning).toBeNull();
    });

    it("returns a mismatch warning when the source table is configured with a different collection", () => {
        const warning = getMissingRelatedCollectionWarningFromAnalysis(
            analyzeRootTables([
                createRootTable({
                    sourceTableSchema: "dbo",
                    sourceTableName: "companies",
                    collectionName: "Businesses",
                }),
            ]),
            {
                linkedCollectionName: "Companies",
                propertyName: "Company",
                sourceTableName: "companies",
                sourceTableSchema: "dbo",
            }
        );

        expect(warning).toBe(
            `Related documents in the "Companies" collection will not be created because the "dbo.companies" root table targets the "Businesses" collection instead.`
        );
    });

    it("collects embedded source table keys from the whole configured tree", () => {
        const analysis = analyzeRootTables([
            createRootTable({
                embeddedTables: [
                    createEmbeddedTable({
                        sourceTableSchema: "dbo",
                        sourceTableName: "order_lines",
                        embeddedTables: [
                            createEmbeddedTable({ sourceTableSchema: "dbo", sourceTableName: "line_notes" }),
                        ],
                    }),
                ],
            }),
        ]);

        expect(analysis.embeddedSourceKeys.has("dbo::order_lines")).toBe(true);
        expect(analysis.embeddedSourceKeys.has("dbo::line_notes")).toBe(true);
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
            `Related documents in the "Media" collection will not be created because "public.media" is not configured as a root table.`,
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
            expect.stringContaining('"public.media" is not configured as a root table'),
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
