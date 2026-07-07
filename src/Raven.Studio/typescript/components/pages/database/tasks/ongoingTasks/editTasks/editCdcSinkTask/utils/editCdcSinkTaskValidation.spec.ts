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
