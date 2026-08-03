import {
    ConnectionStringKey,
    DatabaseSettingKey,
    ImportFromFileFormData,
    OngoingTaskKey,
} from "./importFromFileValidation";
import endpoints = require("endpoints");
import appUrl = require("common/appUrl");

type DatabaseItemType = Raven.Client.Documents.Smuggler.DatabaseItemType;
type DatabaseRecordItemType = Raven.Client.Documents.Smuggler.DatabaseRecordItemType;
type ImportOptions = Raven.Client.Documents.Smuggler.DatabaseSmugglerImportOptions;

export type ImportCommandType = "PowerShell" | "Cmd" | "Bash";

const databaseSettingTokens: Record<DatabaseSettingKey, DatabaseRecordItemType> = {
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

const ongoingTaskTokens: Record<OngoingTaskKey, DatabaseRecordItemType> = {
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

const connectionStringTokens: Record<ConnectionStringKey, DatabaseRecordItemType> = {
    ravenConnectionStrings: "RavenConnectionStrings",
    sqlConnectionStrings: "SqlConnectionStrings",
    snowflakeConnectionStrings: "SnowflakeConnectionStrings",
    olapConnectionStrings: "OlapConnectionStrings",
    elasticSearchConnectionStrings: "ElasticSearchConnectionStrings",
    queueConnectionStrings: "QueueConnectionStrings",
    aiConnectionStrings: "AiConnectionStrings",
};

export function getDefaultFormData(isAdminAccessOrAbove: boolean): ImportFromFileFormData {
    return {
        file: null,
        documents: {
            isIncludeDocuments: true,
            isIncludeAttachments: true,
            isIncludeCounters: true,
            isIncludeRevisions: true,
            isIncludeTimeSeries: true,
            isIncludeTimeSeriesDeletedRanges: true,
            isIncludeArtificialDocuments: false,
            isIncludeArchivedDocuments: true,
            isIncludeExpiredDocuments: true,
            isIncludeConflicts: true,
            isIncludeCompareExchange: true,
            isIncludeLegacyAttachments: false,
            isIncludeDocumentsTombstones: true,
            isIncludeCompareExchangeTombstones: true,
            isIncludeSubscriptions: true,
        },
        collections: {
            isImportAllCollections: true,
            includedCollections: [],
        },
        configuration: {
            isIncludeIndexes: isAdminAccessOrAbove,
            isIncludeIndexHistory: false,
            isRemoveAnalyzers: false,
            isIncludeIdentities: true,
            isIncludeConnectionStringsAndOngoingTasks: true,
            isCustomizeOngoingTasks: false,
            ongoingTasks: {
                periodicBackups: isAdminAccessOrAbove,
                externalReplications: isAdminAccessOrAbove,
                ravenEtls: isAdminAccessOrAbove,
                sqlEtls: isAdminAccessOrAbove,
                snowflakeEtls: isAdminAccessOrAbove,
                olapEtls: isAdminAccessOrAbove,
                elasticSearchEtls: isAdminAccessOrAbove,
                queueEtls: isAdminAccessOrAbove,
                hubReplications: true,
                sinkReplications: isAdminAccessOrAbove,
                embeddingsGeneration: isAdminAccessOrAbove,
                genAi: isAdminAccessOrAbove,
                cdcSinks: isAdminAccessOrAbove,
                aiAgents: isAdminAccessOrAbove,
                remoteAttachments: isAdminAccessOrAbove,
            },
            connectionStrings: {
                ravenConnectionStrings: isAdminAccessOrAbove,
                sqlConnectionStrings: isAdminAccessOrAbove,
                snowflakeConnectionStrings: isAdminAccessOrAbove,
                olapConnectionStrings: isAdminAccessOrAbove,
                elasticSearchConnectionStrings: isAdminAccessOrAbove,
                queueConnectionStrings: isAdminAccessOrAbove,
                aiConnectionStrings: isAdminAccessOrAbove,
            },
            isImportAllSettings: true,
            databaseSettings: {
                settings: true,
                conflictSolverConfig: true,
                client: isAdminAccessOrAbove,
                revisions: isAdminAccessOrAbove,
                refresh: true,
                expiration: isAdminAccessOrAbove,
                documentsCompression: true,
                schemaValidation: true,
                dataArchival: true,
                timeSeries: true,
                sorters: true,
                analyzers: true,
                postgreSqlIntegration: true,
            },
        },
        processing: {
            isUseTransformScript: false,
            transformScript: "",
            isSetMaxReadOpsPerSecond: false,
            maxReadOpsPerSecond: null,
            isEncrypted: false,
            encryptionKey: "",
        },
    };
}

function pushGroupTokens<TKey extends string>(
    tokens: Record<TKey, DatabaseRecordItemType>,
    // yup infers the form groups as index signatures, so literal-keyed Records aren't assignable
    values: Record<string, boolean>,
    includeAll: boolean,
    result: DatabaseRecordItemType[],
    excludedKeys: string[] = []
) {
    (Object.keys(tokens) as TKey[]).forEach((key) => {
        if (excludedKeys.includes(key)) {
            return; // license-restricted entries are never emitted
        }
        if (includeAll || values[key]) {
            result.push(tokens[key]);
        }
    });
}

export function getDatabaseRecordTypes(
    formData: ImportFromFileFormData,
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
        // Knockout parity: non-customized mode
        return configuration.isIncludeIndexHistory ? ["IndexesHistory"] : ["None"];
    }

    // The customized path was entered ONLY because of license restrictions - the user still asked
    // for "import all settings". The server expands "None" to its full default record-type list
    // (which additionally includes LockMode, QueueSinks and IndexesHistory - tokens Studio has no
    // toggle for), so the explicit list emitted here must be "server defaults minus restricted"
    // to avoid silently narrowing the import beyond the restricted features.
    const isRestrictionsOnlyBypass =
        configuration.isImportAllSettings &&
        !configuration.isCustomizeOngoingTasks &&
        configuration.isIncludeConnectionStringsAndOngoingTasks &&
        hasRestrictions;

    const result: DatabaseRecordItemType[] = [];

    (Object.keys(databaseSettingTokens) as (keyof typeof databaseSettingTokens)[]).forEach((key) => {
        if (restrictedSettingKeys.includes(key)) {
            return; // license-restricted settings are never emitted
        }
        if (configuration.isImportAllSettings || configuration.databaseSettings[key]) {
            result.push(databaseSettingTokens[key]);
        }
    });

    if (configuration.isIncludeConnectionStringsAndOngoingTasks) {
        const includeAll = !configuration.isCustomizeOngoingTasks;
        pushGroupTokens(ongoingTaskTokens, configuration.ongoingTasks, includeAll, result, restrictedOngoingTaskKeys);
        pushGroupTokens(
            connectionStringTokens,
            configuration.connectionStrings,
            includeAll,
            result,
            restrictedConnectionStringKeys
        );
    }

    if (isRestrictionsOnlyBypass) {
        // parity with the server's expansion of "None" - tokens Studio has no toggle for
        result.push("LockMode", "QueueSinks");
        result.push("IndexesHistory");
    } else if (configuration.isIncludeIndexHistory) {
        result.push("IndexesHistory");
    }

    return result;
}

export function toImportDto(
    formData: ImportFromFileFormData,
    restrictedSettingKeys: DatabaseSettingKey[] = [],
    restrictedOngoingTaskKeys: OngoingTaskKey[] = [],
    restrictedConnectionStringKeys: ConnectionStringKey[] = []
): ImportOptions {
    const { documents, collections, configuration, processing } = formData;

    const operateOnTypes: DatabaseItemType[] = [];

    const databaseRecordTypes = getDatabaseRecordTypes(
        formData,
        restrictedSettingKeys,
        restrictedOngoingTaskKeys,
        restrictedConnectionStringKeys
    );

    if (databaseRecordTypes.length) {
        operateOnTypes.push("DatabaseRecord");
    }
    if (documents.isIncludeDocuments) {
        operateOnTypes.push("Documents");
    }
    if (documents.isIncludeConflicts) {
        operateOnTypes.push("Conflicts");
    }
    if (configuration.isIncludeIndexes) {
        operateOnTypes.push("Indexes");
    }
    if (documents.isIncludeRevisions) {
        operateOnTypes.push("RevisionDocuments");
    }
    if (configuration.isIncludeIdentities) {
        operateOnTypes.push("Identities");
    }
    if (documents.isIncludeCompareExchange) {
        operateOnTypes.push("CompareExchange");
    }
    if (documents.isIncludeCounters) {
        operateOnTypes.push("CounterGroups");
    }
    if (documents.isIncludeAttachments) {
        operateOnTypes.push("Attachments");
    }
    if (documents.isIncludeLegacyAttachments) {
        operateOnTypes.push("LegacyAttachments");
    }
    if (documents.isIncludeTimeSeries) {
        operateOnTypes.push("TimeSeries");
    }
    if (documents.isIncludeTimeSeriesDeletedRanges) {
        operateOnTypes.push("TimeSeriesDeletedRanges");
    }
    if (documents.isIncludeSubscriptions) {
        operateOnTypes.push("Subscriptions");
    }
    if (documents.isIncludeDocumentsTombstones) {
        operateOnTypes.push("Tombstones");
    }
    if (documents.isIncludeCompareExchangeTombstones) {
        operateOnTypes.push("CompareExchangeTombstones");
    }

    return {
        IncludeExpired: documents.isIncludeExpiredDocuments,
        IncludeArtificial: documents.isIncludeArtificialDocuments,
        IncludeArchived: documents.isIncludeArchivedDocuments,
        TransformScript: processing.isUseTransformScript ? processing.transformScript : "",
        RemoveAnalyzers: configuration.isRemoveAnalyzers,
        EncryptionKey: processing.isEncrypted ? processing.encryptionKey : undefined,
        OperateOnTypes: operateOnTypes.join(",") as DatabaseItemType,
        OperateOnDatabaseRecordTypes: (databaseRecordTypes.length
            ? databaseRecordTypes.join(",")
            : undefined) as DatabaseRecordItemType,
        Collections: collections.isImportAllCollections ? null : collections.includedCollections,
        MaxReadOpsPerSecond: processing.isSetMaxReadOpsPerSecond ? processing.maxReadOpsPerSecond : null,
    } as ImportOptions;
}

export function hasAnyInclude(
    formData: ImportFromFileFormData,
    restrictedSettingKeys: DatabaseSettingKey[] = [],
    restrictedOngoingTaskKeys: OngoingTaskKey[] = [],
    restrictedConnectionStringKeys: ConnectionStringKey[] = []
): boolean {
    const d = formData.documents;
    const c = formData.configuration;
    return (
        d.isIncludeDocuments ||
        d.isIncludeAttachments ||
        d.isIncludeCounters ||
        d.isIncludeRevisions ||
        d.isIncludeTimeSeries ||
        d.isIncludeTimeSeriesDeletedRanges ||
        d.isIncludeArtificialDocuments ||
        d.isIncludeArchivedDocuments ||
        d.isIncludeConflicts ||
        d.isIncludeCompareExchange ||
        d.isIncludeLegacyAttachments ||
        d.isIncludeDocumentsTombstones ||
        d.isIncludeCompareExchangeTombstones ||
        d.isIncludeSubscriptions ||
        c.isIncludeIndexes ||
        c.isIncludeIdentities ||
        c.isIncludeConnectionStringsAndOngoingTasks ||
        getDatabaseRecordTypes(
            formData,
            restrictedSettingKeys,
            restrictedOngoingTaskKeys,
            restrictedConnectionStringKeys
        ).length > 0
    );
}

export function buildImportCurlCommand(
    commandType: ImportCommandType,
    formData: ImportFromFileFormData,
    databaseName: string,
    restrictedSettingKeys: DatabaseSettingKey[] = [],
    restrictedOngoingTaskKeys: OngoingTaskKey[] = [],
    restrictedConnectionStringKeys: ConnectionStringKey[] = []
): string {
    const dto = toImportDto(formData, restrictedSettingKeys, restrictedOngoingTaskKeys, restrictedConnectionStringKeys);
    if (!dto.TransformScript) {
        delete dto.TransformScript;
    }
    const json = JSON.stringify(dto);
    const fileName = formData.file?.name || "Dump of Database.ravendbdump";
    const commandEndpointUrl =
        appUrl.forServer() + appUrl.forDatabaseQuery(databaseName) + endpoints.databases.smuggler.smugglerImport;

    switch (commandType) {
        case "PowerShell":
            return `curl.exe -F 'importOptions=${json.replace(/"/g, '\\"')}' -F 'file=@.\\${fileName}' ${commandEndpointUrl}`;
        case "Cmd":
            return `curl.exe -F "importOptions=${json.replace(/"/g, '\\"')}" -F "file=@.\\${fileName}" ${commandEndpointUrl}`;
        case "Bash":
            return `curl -F 'importOptions=${json}' -F 'file=@${fileName}' ${commandEndpointUrl}`;
    }
}

/**
 * Ongoing tasks that cannot work without their connection string. Importing the task alone leaves a
 * task pointing at a connection string that does not exist in the target database.
 */
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
    formData: Pick<ImportFromFileFormData, "configuration">,
    restrictedOngoingTaskKeys: OngoingTaskKey[] = [],
    restrictedConnectionStringKeys: ConnectionStringKey[] = []
): OngoingTaskKey[] {
    const { configuration } = formData;

    if (!configuration.isIncludeConnectionStringsAndOngoingTasks || !configuration.isCustomizeOngoingTasks) {
        return [];
    }

    return (Object.keys(taskConnectionStringDependencies) as OngoingTaskKey[]).filter((taskKey) => {
        const connectionStringKey = taskConnectionStringDependencies[taskKey];
        return (
            configuration.ongoingTasks[taskKey] &&
            !restrictedOngoingTaskKeys.includes(taskKey) &&
            !restrictedConnectionStringKeys.includes(connectionStringKey) &&
            !configuration.connectionStrings[connectionStringKey]
        );
    });
}

export function getItemsToWarnAbout(formData: Pick<ImportFromFileFormData, "documents">): string[] {
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
