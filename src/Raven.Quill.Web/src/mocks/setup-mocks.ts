import type {
    CdcSinkConfiguration,
    ConnectResult,
    DiscoverResponse,
    ProvisionResponse,
    SuggestCdcResponse,
    TestMappingResponse,
} from "@/api/generated/server-api";
import { apiHttp } from "./api-http";

export const setupMocks = {
    connect: (result: ConnectResult = { success: true, errors: [] }) =>
        apiHttp.post("/api/setup/connect", ({ response }) => response(200).json(result)),
    discover: (discovery: DiscoverResponse = sampleDiscovery) =>
        apiHttp.post("/api/setup/discover", ({ response }) => response(200).json(discovery)),
    map: (configuration: CdcSinkConfiguration = sampleCdcConfiguration) =>
        apiHttp.post("/api/setup/map", ({ response }) => response(200).json(configuration)),
    suggestCdc: (suggestion: SuggestCdcResponse = sampleCdcSuggestion) =>
        apiHttp.post("/api/setup/suggest/cdc", ({ response }) => response(200).json(suggestion)),
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

// Discovery that failed outright: the verify step shows only the destructive error banner.
export const failedDiscovery: DiscoverResponse = {
    catalogName: null,
    success: false,
    hasPermissionToSetup: false,
    errors: [
        'Could not connect to the source database: password authentication failed for user "admin".',
        "Check the connection string and that the database is reachable from the appliance.",
    ],
    warnings: [],
    tables: [],
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
