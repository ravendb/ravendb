import type {
    CdcSinkConfiguration,
    ConnectResult,
    DiscoverResponse,
    ProvisionResponse,
    SuggestCdcResponse,
    TestMappingResponse,
    VerifyCdcResponse,
} from "@/api/generated/server-api";
import { delay } from "msw";
import { apiHttp } from "./api-http";

export const setupMocks = {
    connect: (result: ConnectResult = { success: true, errors: [] }) =>
        apiHttp.post("/api/setup/connect", ({ response }) => response(200).json(result)),
    discover: (discovery: DiscoverResponse = sampleDiscovery) =>
        apiHttp.post("/api/setup/discover", ({ response }) => response(200).json(discovery)),
    verifyCdc: (result: VerifyCdcResponse = sampleCdcVerification) =>
        apiHttp.post("/api/setup/verify-cdc", ({ response }) => response(200).json(result)),
    map: (configuration: CdcSinkConfiguration = sampleCdcConfiguration) =>
        apiHttp.post("/api/setup/map", ({ response }) => response(200).json(configuration)),
    suggestCdc: (suggestion: SuggestCdcResponse = sampleCdcSuggestion) =>
        apiHttp.post("/api/setup/suggest/cdc", ({ response }) => response(200).json(suggestion)),
    /** Never answers, so the map-tables step stays in its "suggesting" state. */
    suggestCdcPending: () =>
        apiHttp.post("/api/setup/suggest/cdc", async ({ response }) => {
            await delay("infinite");
            return response(200).json(sampleCdcSuggestion);
        }),
    testMapping: (result: TestMappingResponse = sampleMappingTest) =>
        apiHttp.post("/api/setup/test-mapping", ({ response }) => response(200).json(result)),
    provision: (result: ProvisionResponse = { id: "apps/1", slug: "demo" }) =>
        apiHttp.post("/api/setup/provision", ({ response }) => response(200).json(result)),
};

export const sampleDiscovery: DiscoverResponse = {
    catalogName: "demo_shop",
    success: true,
    hasPermissionToSetup: true,
    errors: [],
    warnings: [],
    tables: [
        {
            sourceTableSchema: "dbo",
            sourceTableName: "Customers",
            columns: [
                { name: "Id", nativeType: "int", suggestedType: "Default", isPrimaryKey: true, isCdcCapturable: true },
                {
                    name: "Name",
                    nativeType: "nvarchar(200)",
                    suggestedType: "Default",
                    isPrimaryKey: false,
                    isCdcCapturable: true,
                },
                {
                    name: "Preferences",
                    nativeType: "nvarchar(max)",
                    suggestedType: "Json",
                    isPrimaryKey: false,
                    isCdcCapturable: true,
                },
            ],
            primaryKeyColumns: ["Id"],
            foreignKeys: [],
            isCdcEnabled: true,
            warnings: [],
        },
        {
            sourceTableSchema: "dbo",
            sourceTableName: "Orders",
            columns: [
                { name: "Id", nativeType: "int", suggestedType: "Default", isPrimaryKey: true, isCdcCapturable: true },
                {
                    name: "CustomerId",
                    nativeType: "int",
                    suggestedType: "Default",
                    isPrimaryKey: false,
                    isCdcCapturable: true,
                },
                {
                    name: "Total",
                    nativeType: "decimal(18,2)",
                    suggestedType: "Default",
                    isPrimaryKey: false,
                    isCdcCapturable: true,
                },
            ],
            primaryKeyColumns: ["Id"],
            foreignKeys: [
                {
                    columns: ["CustomerId"],
                    referencedSchema: "dbo",
                    referencedTable: "Customers",
                    referencedColumns: ["Id"],
                },
            ],
            isCdcEnabled: true,
            warnings: [],
        },
    ],
};

// Exercises every state the verify step can render: verified tables (clean and with
// table-level warnings), tables that need configuration (CDC disabled vs. an explicit
// unsupported reason), and a response-level warning banner. `hasPermissionToSetup` is
// false so CDC-disabled tables fall into "needs configuration" instead of being verified.
export const discoveryWithAllStates: DiscoverResponse = {
    catalogName: "demo_shop",
    success: true,
    hasPermissionToSetup: false,
    errors: [],
    warnings: ["2 of the discovered tables need configuration before they can be ingested."],
    tables: [
        // Verified, no warnings.
        {
            sourceTableSchema: "dbo",
            sourceTableName: "Customers",
            columns: [
                { name: "Id", nativeType: "int", suggestedType: "Default", isPrimaryKey: true, isCdcCapturable: true },
                {
                    name: "Name",
                    nativeType: "nvarchar(200)",
                    suggestedType: "Default",
                    isPrimaryKey: false,
                    isCdcCapturable: true,
                },
                {
                    name: "Preferences",
                    nativeType: "nvarchar(max)",
                    suggestedType: "Json",
                    isPrimaryKey: false,
                    isCdcCapturable: true,
                },
            ],
            primaryKeyColumns: ["Id", "TenantId", "ExternalReferenceId", "RegionCode"],
            foreignKeys: [],
            isCdcEnabled: true,
            warnings: [],
        },
        // Verified, but carries table-level warnings (amber triangle next to the name).
        {
            sourceTableSchema: "dbo",
            sourceTableName: "Orders",
            columns: [
                { name: "Id", nativeType: "int", suggestedType: "Default", isPrimaryKey: true, isCdcCapturable: true },
                {
                    name: "CustomerId",
                    nativeType: "int",
                    suggestedType: "Default",
                    isPrimaryKey: false,
                    isCdcCapturable: true,
                },
                {
                    name: "Total",
                    nativeType: "decimal(18,2)",
                    suggestedType: "Default",
                    isPrimaryKey: false,
                    isCdcCapturable: true,
                },
                {
                    name: "Notes",
                    nativeType: "text",
                    suggestedType: "Default",
                    isPrimaryKey: false,
                    isCdcCapturable: false,
                    unsupportedReason: "Columns of type 'text' cannot be captured by CDC.",
                },
            ],
            primaryKeyColumns: ["Id"],
            foreignKeys: [
                {
                    columns: ["CustomerId"],
                    referencedSchema: "dbo",
                    referencedTable: "Customers",
                    referencedColumns: ["Id"],
                },
            ],
            isCdcEnabled: true,
            warnings: ['Column "Notes" (text) is not capturable and will be skipped.'],
        },
        // Verified, no warnings (a second valid table in another schema).
        {
            sourceTableSchema: "sales",
            sourceTableName: "Invoices",
            columns: [
                { name: "Id", nativeType: "uuid", suggestedType: "Default", isPrimaryKey: true, isCdcCapturable: true },
                {
                    name: "OrderId",
                    nativeType: "int",
                    suggestedType: "Default",
                    isPrimaryKey: false,
                    isCdcCapturable: true,
                },
                {
                    name: "IssuedAt",
                    nativeType: "timestamptz",
                    suggestedType: "Default",
                    isPrimaryKey: false,
                    isCdcCapturable: true,
                },
            ],
            primaryKeyColumns: ["Id"],
            foreignKeys: [],
            isCdcEnabled: true,
            warnings: [],
        },
        // Needs configuration: CDC is not enabled and the user can't enable it here.
        {
            sourceTableSchema: "dbo",
            sourceTableName: "AuditLog",
            columns: [
                {
                    name: "Id",
                    nativeType: "bigint",
                    suggestedType: "Default",
                    isPrimaryKey: true,
                    isCdcCapturable: false,
                },
                {
                    name: "Action",
                    nativeType: "nvarchar(100)",
                    suggestedType: "Default",
                    isPrimaryKey: false,
                    isCdcCapturable: false,
                },
                {
                    name: "CreatedAt",
                    nativeType: "datetime2",
                    suggestedType: "Default",
                    isPrimaryKey: false,
                    isCdcCapturable: false,
                },
            ],
            primaryKeyColumns: ["Id"],
            foreignKeys: [],
            isCdcEnabled: false,
            warnings: [],
        },
        // Needs configuration: an explicit unsupported reason (no primary key).
        {
            sourceTableSchema: "reporting",
            sourceTableName: "DailySalesView",
            columns: [
                {
                    name: "Day",
                    nativeType: "date",
                    suggestedType: "Default",
                    isPrimaryKey: false,
                    isCdcCapturable: false,
                },
                {
                    name: "Revenue",
                    nativeType: "decimal(18,2)",
                    suggestedType: "Default",
                    isPrimaryKey: false,
                    isCdcCapturable: false,
                },
            ],
            primaryKeyColumns: [],
            foreignKeys: [],
            isCdcEnabled: false,
            unsupportedReason: "Tables without a primary key are not supported by CDC Sink.",
            warnings: [],
        },
    ],
};

export const manyTablesDiscovery: DiscoverResponse = {
    catalogName: "demo_shop",
    success: true,
    hasPermissionToSetup: true,
    errors: [],
    warnings: [],
    tables: Array.from({ length: 80 }, (_, index) => ({
        sourceTableSchema: "dbo",
        sourceTableName: `Table${String(index + 1).padStart(2, "0")}`,
        columns: [
            { name: "Id", nativeType: "int", suggestedType: "Default", isPrimaryKey: true, isCdcCapturable: true },
        ],
        primaryKeyColumns: ["Id"],
        foreignKeys: [],
        isCdcEnabled: true,
        warnings: [],
    })),
};

// Discovery that failed outright: the verify step shows only the destructive error banner.
export const failedDiscovery: DiscoverResponse = {
    catalogName: null,
    success: false,
    hasPermissionToSetup: false,
    errors: [
        {
            message: 'Could not connect to source database: password authentication failed for user "admin"',
            details:
                'Npgsql.NpgsqlException (0x80004005): 28P01: password authentication failed for user "admin"\n' +
                "   at Npgsql.Internal.NpgsqlConnector.<Authenticate>d__0.MoveNext()\n" +
                "   at Npgsql.Internal.NpgsqlConnector.<Open>d__1.MoveNext()",
        },
        {
            message: "Check the connection string and that the database is reachable from the appliance.",
            details: null,
        },
    ],
    warnings: [],
    tables: [],
};

export const sampleCdcVerification: VerifyCdcResponse = {
    success: true,
    errors: [],
    warnings: [],
    completedTables: ["dbo.Customers", "dbo.Orders"],
};

export const failedCdcVerification: VerifyCdcResponse = {
    success: false,
    errors: [
        {
            message: "The database user must have the REPLICATION role attribute to create a replication slot.",
            details:
                "Npgsql.PostgresException (0x80004005): 42501: permission denied to create replication slot\n" +
                "   at Npgsql.Internal.NpgsqlConnector.<ReadMessage>d__0.MoveNext()",
        },
    ],
    warnings: ["source cleanup failed: publication rvn_cdc_p_8f3a was left in place"],
    completedTables: [],
};

export const sampleCdcConfiguration: CdcSinkConfiguration = {
    name: "cdc/demo-shop",
    connectionStringName: "demo-shop-mssql",
    tables: [
        {
            collectionName: "Customers",
            sourceTableSchema: "dbo",
            sourceTableName: "Customers",
            primaryKeyColumns: ["Id"],
            columns: [
                { column: "Name", name: "Name", type: "Default" },
                { column: "Preferences", name: "Preferences", type: "Json" },
            ],
            patch: null,
            onDelete: { ignoreDeletes: false, patch: null },
            disabled: false,
        },
        {
            collectionName: "Orders",
            sourceTableSchema: "dbo",
            sourceTableName: "Orders",
            primaryKeyColumns: ["Id"],
            columns: [
                { column: "CustomerId", name: "CustomerId", type: "Default" },
                { column: "Total", name: "Total", type: "Default" },
            ],
            patch: null,
            onDelete: { ignoreDeletes: false, patch: null },
            disabled: false,
        },
    ],
};

export const sampleCdcSuggestion: SuggestCdcResponse = {
    configuration: sampleCdcConfiguration,
    rationale: ["Customers and Orders have primary keys and CDC enabled, so both can be mapped."],
    status: "Success",
};

export const sampleMappingTest: TestMappingResponse = {
    results: [
        {
            documentId: "Customers/1",
            document: JSON.stringify({ Name: "Aria Stone", Preferences: { newsletter: true } }, null, 4),
            sourceRow: JSON.stringify({ Id: 1, Name: "Aria Stone", Preferences: '{"newsletter":true}' }, null, 4),
            wouldDelete: false,
            debugOutput: [],
        },
    ],
    errors: [],
    warnings: [],
};

export const failedMappingTest: TestMappingResponse = {
    results: [],
    errors: [
        {
            message: 'Could not project column "Preferences" into the mapped document: the value is not valid JSON.',
            details:
                "System.Text.Json.JsonException: '{' is an invalid start of a value. Path: $ | LineNumber: 0\n" +
                "   at System.Text.Json.ThrowHelper.ReThrowWithPath(ReadStack&, JsonReaderException)\n" +
                "   at Raven.Quill.Setup.MappingTester.ProjectColumn(DiscoverColumnResponse, Object)",
        },
        {
            message: "Fix the column mapping or exclude the column, then run the preview again.",
            details: null,
        },
    ],
    warnings: [],
};
