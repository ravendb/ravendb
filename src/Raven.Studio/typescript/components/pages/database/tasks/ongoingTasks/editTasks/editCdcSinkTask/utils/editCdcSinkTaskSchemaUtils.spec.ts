import {
    getRelatedSourceTablesToAdd,
    isTableSupported,
    mapRelatedSqlTablesToFormData,
    mapSourceColumnsToFormData,
    mapSqlTableToFormData,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskSchemaUtils";
import {
    FormEmbeddedTable,
    FormRootTable,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskTypes";

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

    it("maps foreign keys to linked tables by default", () => {
        const schema = createSchema({});
        const table = createTable({ ForeignKeys: [createForeignKey()] });

        expect(mapSqlTableToFormData(schema, table).linkedTables).toEqual([
            {
                propertyName: "Customer",
                joinColumns: [{ value: "customer_id" }],
                linkedCollectionName: "Customers",
                sourceTableName: "customers",
                sourceTableSchema: "dbo",
            },
        ]);
    });
});

describe("mapRelatedSqlTablesToFormData", () => {
    const countriesForeignKey = createForeignKey({ Columns: ["country_id"], ReferencedTable: "countries" });

    it("keeps a link with the conventional name when the referenced table is neither configured nor in the batch", () => {
        const customers = createTable({ SourceTableName: "customers", ForeignKeys: [countriesForeignKey] });
        const schema = createSchema({ Tables: [customers] });

        const [mapped] = mapRelatedSqlTablesToFormData(schema, [], [customers]);

        expect(mapped.linkedTables).toEqual([
            {
                propertyName: "Country",
                joinColumns: [{ value: "country_id" }],
                linkedCollectionName: "Countries",
                sourceTableName: "countries",
                sourceTableSchema: "dbo",
            },
        ]);
    });

    it("keeps a link to a configured root table and aligns the collection name", () => {
        const customers = createTable({ SourceTableName: "customers", ForeignKeys: [countriesForeignKey] });
        const countries = createTable({ SourceTableName: "countries" });
        const schema = createSchema({ Tables: [customers, countries] });
        const countriesRoot = { ...mapSqlTableToFormData(schema, countries), collectionName: "Nations" };

        const [mapped] = mapRelatedSqlTablesToFormData(schema, [countriesRoot], [customers]);

        expect(mapped.linkedTables).toEqual([
            {
                propertyName: "Country",
                joinColumns: [{ value: "country_id" }],
                linkedCollectionName: "Nations",
                sourceTableName: "countries",
                sourceTableSchema: "dbo",
            },
        ]);
    });

    it("keeps links between tables added in the same batch", () => {
        const customers = createTable({ SourceTableName: "customers", ForeignKeys: [countriesForeignKey] });
        const countries = createTable({ SourceTableName: "countries" });
        const schema = createSchema({ Tables: [customers, countries] });

        const mapped = mapRelatedSqlTablesToFormData(schema, [], [customers, countries]);

        expect(mapped[0].linkedTables).toEqual([
            expect.objectContaining({ sourceTableName: "countries", linkedCollectionName: "Countries" }),
        ]);
        expect(mapped[1].linkedTables).toEqual([]);
    });

    it("adopts the collection name the referencing linked table uses", () => {
        const orders = createTable({ SourceTableName: "orders", ForeignKeys: [createForeignKey()] });
        const customers = createTable({ SourceTableName: "customers" });
        const schema = createSchema({ Tables: [orders, customers] });
        const ordersRoot = mapSqlTableToFormData(schema, orders);
        ordersRoot.linkedTables[0].linkedCollectionName = "Clients";

        const [mapped] = mapRelatedSqlTablesToFormData(schema, [ordersRoot], [customers]);

        expect(mapped.collectionName).toBe("Clients");
    });

    it("ignores collection names referenced only from disabled root tables", () => {
        const orders = createTable({ SourceTableName: "orders", ForeignKeys: [createForeignKey()] });
        const customers = createTable({ SourceTableName: "customers" });
        const schema = createSchema({ Tables: [orders, customers] });
        const disabledOrdersRoot = { ...mapSqlTableToFormData(schema, orders), disabled: true };
        disabledOrdersRoot.linkedTables[0].linkedCollectionName = "Clients";

        const [mapped] = mapRelatedSqlTablesToFormData(schema, [disabledOrdersRoot], [customers]);

        expect(mapped.collectionName).toBe("Customers");
    });

    it("falls back to the default collection name when referencing linked tables disagree", () => {
        const orders = createTable({
            SourceTableName: "orders",
            ForeignKeys: [createForeignKey({ Columns: ["ship_to"] }), createForeignKey({ Columns: ["bill_to"] })],
        });
        const customers = createTable({ SourceTableName: "customers" });
        const schema = createSchema({ Tables: [orders, customers] });
        const ordersRoot = mapSqlTableToFormData(schema, orders);
        ordersRoot.linkedTables[0].linkedCollectionName = "Clients";
        ordersRoot.linkedTables[1].linkedCollectionName = "Buyers";

        const [mapped] = mapRelatedSqlTablesToFormData(schema, [ordersRoot], [customers]);

        expect(mapped.collectionName).toBe("Customers");
    });

    it("de-duplicates collection names of same-named tables from different schemas", () => {
        const orders = createTable({
            SourceTableName: "orders",
            ForeignKeys: [
                createForeignKey({ Columns: ["ship_to"], ReferencedSchema: "dbo", ReferencedTable: "customers" }),
                createForeignKey({ Columns: ["bill_to"], ReferencedSchema: "archive", ReferencedTable: "customers" }),
            ],
        });
        const dboCustomers = createTable({ SourceTableName: "customers" });
        const archiveCustomers = createTable({ SourceTableSchema: "archive", SourceTableName: "customers" });
        const schema = createSchema({ Tables: [orders, dboCustomers, archiveCustomers] });

        const mapped = mapRelatedSqlTablesToFormData(
            schema,
            [mapSqlTableToFormData(schema, orders)],
            [dboCustomers, archiveCustomers]
        );

        expect(mapped.map((table) => table.collectionName)).toEqual(["Customers", "ArchiveCustomers"]);
    });

    it("de-duplicates collection names against already configured root tables", () => {
        const orders = createTable({ SourceTableName: "orders", ForeignKeys: [createForeignKey()] });
        const salesCustomers = createTable({ SourceTableSchema: "sales", SourceTableName: "customers" });
        const dboCustomers = createTable({ SourceTableName: "customers" });
        const schema = createSchema({ Tables: [orders, salesCustomers, dboCustomers] });
        const rootTables = [mapSqlTableToFormData(schema, orders), mapSqlTableToFormData(schema, salesCustomers)];

        const [mapped] = mapRelatedSqlTablesToFormData(schema, rootTables, [dboCustomers]);

        expect(mapped.collectionName).toBe("DboCustomers");
    });
});

describe("getRelatedSourceTablesToAdd", () => {
    it("adds source tables referenced by linked tables that are not configured", () => {
        const orders = createTable({
            SourceTableName: "orders",
            ForeignKeys: [createForeignKey({ ReferencedTable: "customers" })],
        });
        const customers = createTable({ SourceTableName: "customers" });
        const schema = createSchema({ Tables: [orders, customers] });

        const result = getRelatedSourceTablesToAdd(schema, [mapSqlTableToFormData(schema, orders)]);

        expect(result.map((table) => table.SourceTableName)).toEqual(["customers"]);
    });

    it("adds a related table only once when referenced by multiple linked tables", () => {
        const orders = createTable({
            SourceTableName: "orders",
            ForeignKeys: [
                createForeignKey({ Columns: ["ship_to"], ReferencedTable: "customers" }),
                createForeignKey({ Columns: ["bill_to"], ReferencedTable: "customers" }),
            ],
        });
        const customers = createTable({ SourceTableName: "customers" });
        const schema = createSchema({ Tables: [orders, customers] });

        const result = getRelatedSourceTablesToAdd(schema, [mapSqlTableToFormData(schema, orders)]);

        expect(result.map((table) => table.SourceTableName)).toEqual(["customers"]);
    });

    it("skips related tables that are already configured as root tables", () => {
        const orders = createTable({
            SourceTableName: "orders",
            ForeignKeys: [createForeignKey({ ReferencedTable: "customers" })],
        });
        const customers = createTable({ SourceTableName: "customers" });
        const schema = createSchema({ Tables: [orders, customers] });

        const result = getRelatedSourceTablesToAdd(schema, [
            mapSqlTableToFormData(schema, orders),
            mapSqlTableToFormData(schema, customers),
        ]);

        expect(result).toEqual([]);
    });

    it("matches references case-insensitively", () => {
        const orders = createTable({
            SourceTableName: "orders",
            ForeignKeys: [createForeignKey({ ReferencedSchema: "DBO", ReferencedTable: "Customers" })],
        });
        const customers = createTable({ SourceTableSchema: "dbo", SourceTableName: "customers" });
        const schema = createSchema({ Tables: [orders, customers] });

        const result = getRelatedSourceTablesToAdd(schema, [mapSqlTableToFormData(schema, orders)]);

        expect(result.map((table) => table.SourceTableName)).toEqual(["customers"]);
    });

    it("follows foreign keys of the referenced tables transitively", () => {
        const orders = createTable({
            SourceTableName: "orders",
            ForeignKeys: [createForeignKey({ ReferencedTable: "customers" })],
        });
        const customers = createTable({
            SourceTableName: "customers",
            ForeignKeys: [createForeignKey({ Columns: ["country_id"], ReferencedTable: "countries" })],
        });
        const countries = createTable({ SourceTableName: "countries" });
        const schema = createSchema({ Tables: [orders, customers, countries] });

        const result = getRelatedSourceTablesToAdd(schema, [mapSqlTableToFormData(schema, orders)]);

        expect(result.map((table) => table.SourceTableName)).toEqual(["customers", "countries"]);
    });

    it("terminates when the foreign keys form a cycle", () => {
        const orders = createTable({
            SourceTableName: "orders",
            ForeignKeys: [createForeignKey({ ReferencedTable: "customers" })],
        });
        const customers = createTable({
            SourceTableName: "customers",
            ForeignKeys: [
                createForeignKey({ Columns: ["last_order_id"], ReferencedTable: "orders" }),
                createForeignKey({ Columns: ["parent_id"], ReferencedTable: "customers" }),
            ],
        });
        const schema = createSchema({ Tables: [orders, customers] });

        const result = getRelatedSourceTablesToAdd(schema, [mapSqlTableToFormData(schema, orders)]);

        expect(result.map((table) => table.SourceTableName)).toEqual(["customers"]);
    });

    it("does not traverse foreign keys of unsupported referenced tables", () => {
        const orders = createTable({
            SourceTableName: "orders",
            ForeignKeys: [createForeignKey({ ReferencedTable: "customers" })],
        });
        const customers = createTable({
            SourceTableName: "customers",
            IsCdcEnabled: false,
            ForeignKeys: [createForeignKey({ Columns: ["country_id"], ReferencedTable: "countries" })],
        });
        const countries = createTable({ SourceTableName: "countries" });
        const schema = createSchema({ HasPermissionToSetup: false, Tables: [orders, customers, countries] });

        expect(getRelatedSourceTablesToAdd(schema, [mapSqlTableToFormData(schema, orders)])).toEqual([]);
    });

    it("does not add related tables that are unsupported", () => {
        const orders = createTable({
            SourceTableName: "orders",
            ForeignKeys: [createForeignKey({ ReferencedTable: "customers" })],
        });
        const customers = createTable({ SourceTableName: "customers", IsCdcEnabled: false });
        const schema = createSchema({ HasPermissionToSetup: false, Tables: [orders, customers] });

        const result = getRelatedSourceTablesToAdd(schema, [mapSqlTableToFormData(schema, orders)]);

        expect(result).toEqual([]);
    });

    it("returns nothing when the source schema is unavailable", () => {
        expect(getRelatedSourceTablesToAdd(null, [createLinkedTableOwner()])).toEqual([]);
    });

    it("skips related tables that are already configured as embedded tables", () => {
        const orders = createTable({
            SourceTableName: "orders",
            ForeignKeys: [createForeignKey({ ReferencedTable: "customers" })],
        });
        const customers = createTable({ SourceTableName: "customers" });
        const schema = createSchema({ Tables: [orders, customers] });
        const ordersRoot: FormRootTable = {
            ...mapSqlTableToFormData(schema, orders),
            embeddedTables: [createEmbeddedTableConfig({ sourceTableSchema: "dbo", sourceTableName: "customers" })],
        };

        expect(getRelatedSourceTablesToAdd(schema, [ordersRoot])).toEqual([]);
    });

    it("skips related tables that are configured as disabled root tables", () => {
        const orders = createTable({
            SourceTableName: "orders",
            ForeignKeys: [createForeignKey({ ReferencedTable: "customers" })],
        });
        const customers = createTable({ SourceTableName: "customers" });
        const schema = createSchema({ Tables: [orders, customers] });
        const disabledCustomersRoot = { ...mapSqlTableToFormData(schema, customers), disabled: true };

        expect(
            getRelatedSourceTablesToAdd(schema, [mapSqlTableToFormData(schema, orders), disabledCustomersRoot])
        ).toEqual([]);
    });

    it("ignores linked-table references inside disabled root tables", () => {
        const orders = createTable({
            SourceTableName: "orders",
            ForeignKeys: [createForeignKey({ ReferencedTable: "customers" })],
        });
        const customers = createTable({ SourceTableName: "customers" });
        const schema = createSchema({ Tables: [orders, customers] });
        const disabledOrdersRoot = { ...mapSqlTableToFormData(schema, orders), disabled: true };

        expect(getRelatedSourceTablesToAdd(schema, [disabledOrdersRoot])).toEqual([]);
    });
});

function createEmbeddedTableConfig(overrides: Partial<FormEmbeddedTable>): FormEmbeddedTable {
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
        propertyName: "Items",
        sourceTableName: "order_lines",
        sourceTableSchema: "dbo",
        type: "Array",
        ...overrides,
    };
}

function createLinkedTableOwner() {
    const schema = createSchema({
        Tables: [
            createTable({
                SourceTableName: "orders",
                ForeignKeys: [createForeignKey({ ReferencedTable: "customers" })],
            }),
        ],
    });

    return mapSqlTableToFormData(schema, schema.Tables[0]);
}

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

function createForeignKey(
    overrides: Partial<CdcSinkSchema.CdcSinkSourceForeignKey> = {}
): CdcSinkSchema.CdcSinkSourceForeignKey {
    return {
        Columns: ["customer_id"],
        ReferencedColumns: ["Id"],
        ReferencedSchema: "dbo",
        ReferencedTable: "customers",
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
