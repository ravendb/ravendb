import {
    ConnectionStringKey,
    connectionStringKeys,
    DatabaseSettingKey,
    databaseSettingKeys,
    ImportOptionsFormData,
    OngoingTaskKey,
    ongoingTaskKeys,
} from "components/pages/database/tasks/importData/shared/importOptionsValidation";

type DatabaseRecordItemType = Raven.Client.Documents.Smuggler.DatabaseRecordItemType;
type ImportConfiguration = ImportOptionsFormData["configuration"];

export const databaseSettingTokens: Record<DatabaseSettingKey, DatabaseRecordItemType> = {
    settings: "Settings",
    conflictSolverConfig: "ConflictSolverConfig",
    client: "Client",
    revisions: "Revisions",
    refresh: "Refresh",
    expiration: "Expiration",
    documentsCompression: "DocumentsCompression",
    schemaValidation: "SchemaValidation",
    dataArchival: "DataArchival",
    timeSeries: "TimeSeries",
    sorters: "Sorters",
    analyzers: "Analyzers",
    postgreSqlIntegration: "PostgreSQLIntegration",
};

export const ongoingTaskTokens: Record<OngoingTaskKey, DatabaseRecordItemType> = {
    periodicBackups: "PeriodicBackups",
    externalReplications: "ExternalReplications",
    ravenEtls: "RavenEtls",
    sqlEtls: "SqlEtls",
    snowflakeEtls: "SnowflakeEtls",
    olapEtls: "OlapEtls",
    elasticSearchEtls: "ElasticSearchEtls",
    queueEtls: "QueueEtls",
    hubReplications: "HubPullReplications",
    sinkReplications: "SinkPullReplications",
    embeddingsGeneration: "EmbeddingsGenerations",
    genAi: "GenAiEtls",
    cdcSinks: "CdcSinks",
    aiAgents: "AiAgents",
    remoteAttachments: "RemoteAttachments",
};

export const connectionStringTokens: Record<ConnectionStringKey, DatabaseRecordItemType> = {
    ravenConnectionStrings: "RavenConnectionStrings",
    sqlConnectionStrings: "SqlConnectionStrings",
    snowflakeConnectionStrings: "SnowflakeConnectionStrings",
    olapConnectionStrings: "OlapConnectionStrings",
    elasticSearchConnectionStrings: "ElasticSearchConnectionStrings",
    queueConnectionStrings: "QueueConnectionStrings",
    aiConnectionStrings: "AiConnectionStrings",
};

function pushGroupTokens<TKey extends string>(
    keys: readonly TKey[],
    tokens: Record<TKey, DatabaseRecordItemType>,
    values: Partial<Record<TKey, boolean>>,
    includeAll: boolean,
    result: DatabaseRecordItemType[],
    excludedKeys: readonly TKey[]
) {
    keys.forEach((key) => {
        if (excludedKeys.includes(key)) {
            return;
        }
        if (includeAll || values[key]) {
            result.push(tokens[key]);
        }
    });
}

export function getDatabaseRecordTypes(
    formData: Pick<ImportOptionsFormData, "configuration">,
    restrictedSettingKeys: DatabaseSettingKey[] = [],
    restrictedOngoingTaskKeys: OngoingTaskKey[] = [],
    restrictedConnectionStringKeys: ConnectionStringKey[] = []
): DatabaseRecordItemType[] {
    const { configuration } = formData;

    const hasRestrictions =
        restrictedSettingKeys.length > 0 ||
        restrictedOngoingTaskKeys.length > 0 ||
        restrictedConnectionStringKeys.length > 0;

    const isCustomized =
        !configuration.isImportAllSettings ||
        configuration.isCustomizeOngoingTasks ||
        !configuration.isIncludeConnectionStringsAndOngoingTasks ||
        hasRestrictions;

    if (!isCustomized) {
        return configuration.isIncludeIndexHistory ? ["IndexesHistory"] : ["None"];
    }

    const isRestrictionsOnlyBypass =
        configuration.isImportAllSettings &&
        !configuration.isCustomizeOngoingTasks &&
        configuration.isIncludeConnectionStringsAndOngoingTasks &&
        hasRestrictions;

    const result: DatabaseRecordItemType[] = [];

    pushGroupTokens(
        databaseSettingKeys,
        databaseSettingTokens,
        configuration.databaseSettings,
        configuration.isImportAllSettings,
        result,
        restrictedSettingKeys
    );

    if (configuration.isIncludeConnectionStringsAndOngoingTasks) {
        const includeAll = !configuration.isCustomizeOngoingTasks;
        pushGroupTokens(
            ongoingTaskKeys,
            ongoingTaskTokens,
            configuration.ongoingTasks,
            includeAll,
            result,
            restrictedOngoingTaskKeys
        );
        pushGroupTokens(
            connectionStringKeys,
            connectionStringTokens,
            configuration.connectionStrings,
            includeAll,
            result,
            restrictedConnectionStringKeys
        );
    }

    if (isRestrictionsOnlyBypass) {
        // the server expands "None" to these too, and Studio has no toggle for them
        result.push("LockMode", "QueueSinks", "IndexesHistory");
    } else if (configuration.isIncludeIndexHistory) {
        result.push("IndexesHistory");
    }

    return result;
}

const taskConnectionStringDependencies: Partial<Record<OngoingTaskKey, ConnectionStringKey>> = {
    externalReplications: "ravenConnectionStrings",
    hubReplications: "ravenConnectionStrings",
    sinkReplications: "ravenConnectionStrings",
    ravenEtls: "ravenConnectionStrings",
    sqlEtls: "sqlConnectionStrings",
    snowflakeEtls: "snowflakeConnectionStrings",
    olapEtls: "olapConnectionStrings",
    elasticSearchEtls: "elasticSearchConnectionStrings",
    queueEtls: "queueConnectionStrings",
    cdcSinks: "queueConnectionStrings",
    genAi: "aiConnectionStrings",
    aiAgents: "aiConnectionStrings",
    embeddingsGeneration: "aiConnectionStrings",
};

export function getTasksMissingConnectionStrings(
    configuration: Pick<
        ImportConfiguration,
        "isIncludeConnectionStringsAndOngoingTasks" | "isCustomizeOngoingTasks" | "ongoingTasks" | "connectionStrings"
    >,
    restrictedOngoingTaskKeys: OngoingTaskKey[] = [],
    restrictedConnectionStringKeys: ConnectionStringKey[] = []
): OngoingTaskKey[] {
    if (!configuration.isIncludeConnectionStringsAndOngoingTasks || !configuration.isCustomizeOngoingTasks) {
        return [];
    }

    return ongoingTaskKeys.filter((taskKey) => {
        const connectionStringKey = taskConnectionStringDependencies[taskKey];
        return (
            connectionStringKey &&
            configuration.ongoingTasks[taskKey] &&
            !restrictedOngoingTaskKeys.includes(taskKey) &&
            !restrictedConnectionStringKeys.includes(connectionStringKey) &&
            !configuration.connectionStrings[connectionStringKey]
        );
    });
}

export function getItemsToWarnAbout(formData: Pick<ImportOptionsFormData, "documents">): string[] {
    const d = formData.documents;
    if (d.isIncludeDocuments) {
        return [];
    }
    const items: string[] = [];
    if (d.isIncludeCounters) {
        items.push("Counters");
    }
    if (d.isIncludeTimeSeries) {
        items.push("Time Series");
    }
    if (d.isIncludeRevisions) {
        items.push("Revisions");
    }
    return items;
}
