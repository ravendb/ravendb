import {
    EditCdcSinkTaskFormData,
    editCdcSinkTaskSchema,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";

describe("editCdcSinkTaskSchema", () => {
    it("rejects duplicate root source table configurations", async () => {
        const formData = createFormData({
            tables: [
                createRootTable({
                    collectionName: "Orders",
                    sourceTableSchema: "dbo",
                    sourceTableName: "orders",
                }),
                createRootTable({
                    collectionName: "Cars",
                    sourceTableSchema: "DBO",
                    sourceTableName: "Orders",
                }),
            ],
        });

        await expect(editCdcSinkTaskSchema.validate(formData, { abortEarly: false })).rejects.toMatchObject({
            inner: expect.arrayContaining([
                expect.objectContaining({
                    path: "tables.0.sourceTableName",
                    message: expect.stringContaining('"dbo.orders"'),
                }),
                expect.objectContaining({
                    path: "tables.1.sourceTableName",
                    message: expect.stringContaining('"DBO.Orders"'),
                }),
            ]),
        });
    });

    it("rejects a duplicate root source table even when one of them is disabled", async () => {
        const formData = createFormData({
            tables: [
                createRootTable({
                    collectionName: "Orders",
                    sourceTableSchema: "dbo",
                    sourceTableName: "orders",
                }),
                createRootTable({
                    collectionName: "Cars",
                    sourceTableSchema: "dbo",
                    sourceTableName: "orders",
                    disabled: true,
                }),
            ],
        });

        await expect(editCdcSinkTaskSchema.validate(formData, { abortEarly: false })).rejects.toMatchObject({
            inner: expect.arrayContaining([
                expect.objectContaining({
                    path: "tables.0.sourceTableName",
                    message: expect.stringContaining('"dbo.orders"'),
                }),
                expect.objectContaining({
                    path: "tables.1.sourceTableName",
                    message: expect.stringContaining('"dbo.orders"'),
                }),
            ]),
        });
    });

    it("rejects empty source schema for root, embedded, and linked table configs", async () => {
        const formData = createFormData({
            tables: [
                createRootTable({
                    sourceTableSchema: "",
                    embeddedTables: [createEmbeddedTable({ sourceTableSchema: "" })],
                    linkedTables: [createLinkedTable({ sourceTableSchema: "" })],
                }),
            ],
        });

        await expect(editCdcSinkTaskSchema.validate(formData, { abortEarly: false })).rejects.toMatchObject({
            inner: expect.arrayContaining([
                expect.objectContaining({
                    path: "tables[0].sourceTableSchema",
                    message: expect.stringContaining("Source schema is required"),
                }),
                expect.objectContaining({
                    path: "tables[0].embeddedTables[0].sourceTableSchema",
                    message: expect.stringContaining("Source schema is required"),
                }),
                expect.objectContaining({
                    path: "tables[0].linkedTables[0].sourceTableSchema",
                    message: expect.stringContaining("Source schema is required"),
                }),
            ]),
        });
    });
});

function createFormData(overrides: Partial<EditCdcSinkTaskFormData>): EditCdcSinkTaskFormData {
    return {
        name: "Task",
        state: "Enabled",
        isSetResponsibleNode: false,
        responsibleNode: "",
        isPinResponsibleNode: false,
        connectionStringName: "sql-name",
        skipInitialLoad: false,
        postgresPublicationName: "",
        postgresSlotName: "",
        tables: [createRootTable()],
        ...overrides,
    };
}

function createRootTable(
    overrides: Partial<EditCdcSinkTaskFormData["tables"][number]> = {}
): EditCdcSinkTaskFormData["tables"][number] {
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

function createEmbeddedTable(
    overrides: Partial<EditCdcSinkTaskFormData["tables"][number]["embeddedTables"][number]> = {}
): EditCdcSinkTaskFormData["tables"][number]["embeddedTables"][number] {
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
        joinColumns: [{ value: "OrderId" }],
        linkedTables: [],
        onDelete: {
            ignoreDeletes: false,
            patch: "",
        },
        patch: "",
        primaryKeyColumns: [{ value: "Id" }],
        propertyName: "Lines",
        sourceTableName: "order_lines",
        sourceTableSchema: "dbo",
        type: "Array",
        ...overrides,
    };
}

function createLinkedTable(
    overrides: Partial<EditCdcSinkTaskFormData["tables"][number]["linkedTables"][number]> = {}
): EditCdcSinkTaskFormData["tables"][number]["linkedTables"][number] {
    return {
        joinColumns: [{ value: "CompanyId" }],
        linkedCollectionName: "Companies",
        propertyName: "Company",
        sourceTableName: "companies",
        sourceTableSchema: "dbo",
        ...overrides,
    };
}
