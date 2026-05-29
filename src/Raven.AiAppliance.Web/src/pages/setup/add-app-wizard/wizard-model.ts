import type {
    CdcSinkTableConfig,
    ConnectRequest,
    DiscoverResponse,
    DiscoverTableResponse,
    MapRequest,
} from "@/api/generated/server-api";

export const DEFAULT_TEST_ROWS = 5;

export type SetupWizardFormValues = {
    appName: string;
    connectionString: string;
    dataSource: "external";
    mappingMode: "auto";
    provider: string;
};

export const SETUP_WIZARD_STEPS = [
    {
        id: "choose-source",
        label: "Choose data source",
    },
    {
        id: "connect-source",
        label: "Connect to your source database",
    },
    {
        id: "verify-schema",
        label: "Verify your schema",
    },
    {
        id: "map-schema",
        label: "Map your schema",
    },
    {
        id: "preview",
        label: "Preview",
    },
] as const;

export type SetupWizardStepId = (typeof SETUP_WIZARD_STEPS)[number]["id"];

export type SetupWizardMessage = {
    description?: string;
    title: string;
    type: "error" | "success";
};

export const WIZARD_STEP_ORDER = SETUP_WIZARD_STEPS.map((step) => step.id);

export const DATA_SOURCE_OPTIONS = [
    {
        description: "Connect to an existing database on your RavenDB server.",
        disabled: true,
        id: "ravendb",
        label: "RavenDB database",
    },
    {
        description: "Mirror data from PostgreSQL, SQL Server, or MySQL via Change Data Capture.",
        disabled: false,
        id: "external",
        label: "External database",
    },
] as const;

export const PROVIDER_OPTIONS = [
    {
        description: "Npgsql",
        label: "PostgreSQL",
        value: "Npgsql",
    },
    {
        description: "SqlClient",
        label: "SQL Server",
        value: "SqlClient",
    },
    {
        description: "MySqlConnectorFactory",
        label: "MySQL",
        value: "MySqlConnectorFactory",
    },
] as const;

export const MAPPING_MODE_OPTIONS = [
    {
        description: "Generate a CDC mapping from the discovered schema.",
        disabled: false,
        id: "auto",
        label: "Auto",
    },
    {
        description: "Suggest a mapping from application intent.",
        disabled: true,
        id: "ai-suggest",
        label: "AI Suggest",
    },
    {
        description: "Build the mapping table by table.",
        disabled: true,
        id: "manual",
        label: "Manual",
    },
    {
        description: "Use an existing CDC mapping file.",
        disabled: true,
        id: "import",
        label: "Import",
    },
] as const;

export function getInitialFormValues(): SetupWizardFormValues {
    return {
        appName: "",
        connectionString: "",
        dataSource: "external",
        mappingMode: "auto",
        provider: PROVIDER_OPTIONS[0].value,
    };
}

export function getStepIndex(stepId: SetupWizardStepId) {
    return WIZARD_STEP_ORDER.indexOf(stepId);
}

export function getVisibleWizardSteps(currentStep: SetupWizardStepId) {
    if (currentStep === "choose-source") {
        return SETUP_WIZARD_STEPS;
    }

    return SETUP_WIZARD_STEPS.filter((step) => step.id !== "choose-source");
}

export function getNextStep(stepId: SetupWizardStepId) {
    return WIZARD_STEP_ORDER[getStepIndex(stepId) + 1] ?? stepId;
}

export function getPreviousStep(stepId: SetupWizardStepId) {
    return WIZARD_STEP_ORDER[getStepIndex(stepId) - 1] ?? stepId;
}

export function toConnectRequest(values: SetupWizardFormValues): ConnectRequest {
    return {
        connectionString: values.connectionString.trim(),
        provider: values.provider,
        tableNames: null,
    };
}

export function toVerifyConnectRequest(values: SetupWizardFormValues, schema: DiscoverResponse): ConnectRequest {
    return {
        connectionString: values.connectionString.trim(),
        provider: values.provider,
        tableNames: getDiscoveredTableNames(schema),
    };
}

export function buildAutoConfiguration(schema: DiscoverResponse, selectedTableKeys?: string[]): MapRequest {
    const selectedTables = selectedTableKeys ? new Set(selectedTableKeys) : null;
    const tables = schema.tables
        .filter((table) => isTableUsable(table) && (!selectedTables || selectedTables.has(getTableKey(table))))
        .map((table) => ({
            collectionName: toCollectionName(table.sourceTableName),
            columns: table.columns
                .filter((column) => column.isCdcCapturable)
                .map((column) => ({
                    column: column.name,
                    name: toPropertyName(column.name),
                    type: toColumnMappingType(column.suggestedType),
                })),
            disabled: false,
            embeddedTables: [],
            linkedTables: [],
            onDelete: {
                ignoreDeletes: false,
            },
            primaryKeyColumns: table.primaryKeyColumns,
            sourceTableName: table.sourceTableName,
            sourceTableSchema: table.sourceTableSchema,
        }))
        .filter((table) => table.primaryKeyColumns.length > 0 && table.columns.length > 0);

    return {
        tables,
    };
}

export function isTableUsable(table: DiscoverTableResponse) {
    return table.isCdcEnabled && !table.unsupportedReason;
}

export function getTableKey(table: DiscoverTableResponse) {
    return table.sourceTableSchema ? `${table.sourceTableSchema}.${table.sourceTableName}` : table.sourceTableName;
}

export function getMappedTableKey(table: CdcSinkTableConfig) {
    const sourceTableName = table.sourceTableName ?? "";

    return table.sourceTableSchema ? `${table.sourceTableSchema}.${sourceTableName}` : sourceTableName;
}

export function getTableLabel(table: DiscoverTableResponse) {
    return getTableKey(table);
}

export function getPrimaryKeyLabel(table: Pick<DiscoverTableResponse, "primaryKeyColumns">) {
    return table.primaryKeyColumns.length ? table.primaryKeyColumns.join(", ") : "None";
}

export function isConnectSuccess(result: { success: boolean; errors: string[] }) {
    return result.success && result.errors.length === 0;
}

export function firstMessage(messages: string[]) {
    return messages.find(Boolean);
}

function getDiscoveredTableNames(schema: DiscoverResponse) {
    return [...new Set(schema.tables.map((table) => table.sourceTableName).filter(Boolean))];
}

function toColumnMappingType(type: DiscoverTableResponse["columns"][number]["suggestedType"]) {
    if (type === "Json") {
        return "Json" as const;
    }

    if (type === "Attachment") {
        return "Attachment" as const;
    }

    return "Default" as const;
}

function toCollectionName(value: string) {
    const normalized = toPascalCase(value);
    return normalized ? `${normalized[0]?.toUpperCase() ?? ""}${normalized.slice(1)}` : value;
}

function toPropertyName(value: string) {
    const normalized = toPascalCase(value);
    return normalized ? `${normalized[0]?.toLowerCase() ?? ""}${normalized.slice(1)}` : value;
}

function toPascalCase(value: string) {
    return value
        .split(/[^a-zA-Z0-9]+/)
        .filter(Boolean)
        .map((part) => `${part[0]?.toUpperCase() ?? ""}${part.slice(1)}`)
        .join("");
}
