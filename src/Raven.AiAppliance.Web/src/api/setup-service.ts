import type { ApiClient } from "@/api/http-client";

export type ConnectRequest = {
    provider: string;
    connectionString: string;
    tableNames?: string[] | null;
};

export type ConnectResult = {
    success: boolean;
    hasPermissionToSetup: boolean;
    errors: string[];
    warnings: string[];
};

export type CdcColumnType = "Default" | "Json" | "Attachment" | 0 | 1 | 2;

export type CdcSinkSourceColumn = {
    name: string;
    nativeType: string;
    suggestedType: CdcColumnType;
    isPrimaryKey: boolean;
    isCdcCapturable: boolean;
    unsupportedReason?: string | null;
};

export type CdcSinkSourceTable = {
    sourceTableSchema?: string | null;
    sourceTableName: string;
    columns: CdcSinkSourceColumn[];
    primaryKeyColumns: string[];
    isCdcEnabled: boolean;
    unsupportedReason?: string | null;
};

export type CdcSinkSourceSchema = {
    catalogName?: string | null;
    tables: CdcSinkSourceTable[];
    errors: string[];
};

export type CdcColumnMapping = {
    column: string;
    name: string;
    type?: 1 | 2;
};

export type CdcSinkTableConfig = {
    collectionName: string;
    sourceTableSchema?: string | null;
    sourceTableName: string;
    columns: CdcColumnMapping[];
    primaryKeyColumns: string[];
    disabled?: boolean;
    embeddedTables?: unknown[];
    linkedTables?: unknown[];
};

export type CdcSinkConfiguration = {
    name?: string;
    disabled?: boolean;
    connectionStringName?: string;
    tables: CdcSinkTableConfig[];
    skipInitialLoad?: boolean;
};

export type TestMappingRequest = {
    sourceTableName: string;
    maxRows?: number | null;
    sourceTableSchema?: string | null;
};

export type TestMappingRowResult = {
    documentId?: string | null;
    document?: string | null;
    sourceRow?: string | null;
    wouldDelete: boolean;
    ignoreDeletes: boolean;
    debugOutput?: string[] | null;
    error?: string | null;
};

export type TestMappingResult = {
    results: TestMappingRowResult[];
    errors: string[];
    warnings: string[];
};

export type ProvisionRequest = {
    appName: string;
};

export type ProvisionResult = {
    id: string;
    slug: string;
};

export function createSetupService(client: ApiClient) {
    return {
        connect: (request: ConnectRequest) => client.post<ConnectResult>("/setup/connect", request),
        discover: (request: ConnectRequest) => client.post<CdcSinkSourceSchema>("/setup/discover", request),
        map: (configuration: CdcSinkConfiguration) => client.post<CdcSinkConfiguration>("/setup/map", configuration),
        testMapping: (request: TestMappingRequest) => client.post<TestMappingResult>("/setup/test-mapping", request),
        provision: (request: ProvisionRequest) => client.post<ProvisionResult>("/setup/provision", request),
    };
}

export type SetupService = ReturnType<typeof createSetupService>;
