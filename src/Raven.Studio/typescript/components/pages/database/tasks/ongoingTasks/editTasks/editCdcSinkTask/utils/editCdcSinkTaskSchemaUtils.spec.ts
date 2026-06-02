import {
    isTableSupported,
    mapSourceColumnsToFormData,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskSchemaUtils";

import CdcSinkSchema = Raven.Client.Documents.Operations.CdcSink.Schema;

describe("CDC Sink schema utils", () => {
    it("allows an already enabled table without setup permissions", () => {
        const schema = createSchema({ HasPermissionToSetup: false });
        const table = createTable({ IsCdcEnabled: true });

        expect(isTableSupported(schema, table)).toBe(true);
    });

    it("allows a disabled table when CDC can be enabled automatically", () => {
        const schema = createSchema({ HasPermissionToSetup: true });
        const table = createTable({ IsCdcEnabled: false });

        expect(isTableSupported(schema, table)).toBe(true);
    });

    it("blocks a disabled table when CDC cannot be enabled automatically", () => {
        const schema = createSchema({ HasPermissionToSetup: false });
        const table = createTable({ IsCdcEnabled: false });

        expect(isTableSupported(schema, table)).toBe(false);
    });

    it("blocks a table with an explicit unsupported reason", () => {
        const schema = createSchema({ HasPermissionToSetup: true });
        const table = createTable({ UnsupportedReason: "This table cannot be captured." });

        expect(isTableSupported(schema, table)).toBe(false);
    });

    it("blocks tables when connection-level verification fails", () => {
        const schema = createSchema({ Success: false });
        const table = createTable({ IsCdcEnabled: true });

        expect(isTableSupported(schema, table)).toBe(false);
    });

    it("maps temporarily uncapturable columns when CDC will be enabled automatically", () => {
        const schema = createSchema({ HasPermissionToSetup: true });
        const table = createTable({
            IsCdcEnabled: false,
            Columns: [createColumn({ IsCdcCapturable: false, UnsupportedReason: "CDC is not enabled." })],
        });

        expect(mapSourceColumnsToFormData(schema, table)).toEqual([
            {
                column: "Id",
                name: "Id",
                type: "Default",
            },
        ]);
    });

    it("omits uncapturable columns from an already enabled table", () => {
        const schema = createSchema({ HasPermissionToSetup: true });
        const table = createTable({
            IsCdcEnabled: true,
            Columns: [createColumn({ IsCdcCapturable: false, UnsupportedReason: "Column is not captured." })],
        });

        expect(mapSourceColumnsToFormData(schema, table)).toEqual([]);
    });
});

function createSchema(overrides: Partial<CdcSinkSchema.CdcSinkSourceSchema>): CdcSinkSchema.CdcSinkSourceSchema {
    return {
        CatalogName: "Northwind",
        Errors: [],
        HasPermissionToSetup: true,
        Success: true,
        Tables: [],
        Warnings: [],
        ...overrides,
    };
}

function createTable(overrides: Partial<CdcSinkSchema.CdcSinkSourceTable>): CdcSinkSchema.CdcSinkSourceTable {
    return {
        Columns: [createColumn()],
        ForeignKeys: [],
        IsCdcEnabled: true,
        PrimaryKeyColumns: ["Id"],
        SourceTableName: "orders",
        SourceTableSchema: "dbo",
        UnsupportedReason: null,
        Warnings: [],
        ...overrides,
    };
}

function createColumn(overrides: Partial<CdcSinkSchema.CdcSinkSourceColumn> = {}): CdcSinkSchema.CdcSinkSourceColumn {
    return {
        IsCdcCapturable: true,
        IsPrimaryKey: true,
        Name: "Id",
        NativeType: "int",
        SuggestedType: "Default",
        UnsupportedReason: null,
        ...overrides,
    };
}
