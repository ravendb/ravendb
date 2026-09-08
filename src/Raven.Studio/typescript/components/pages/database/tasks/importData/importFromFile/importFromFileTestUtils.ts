type ImportOptions = Raven.Client.Documents.Smuggler.DatabaseSmugglerImportOptions;
type DatabaseItemType = Raven.Client.Documents.Smuggler.DatabaseItemType;
type DatabaseRecordItemType = Raven.Client.Documents.Smuggler.DatabaseRecordItemType;

export const adminDefaultOperateOnTypes: DatabaseItemType[] = [
    "DatabaseRecord",
    "Documents",
    "Conflicts",
    "Indexes",
    "RevisionDocuments",
    "Identities",
    "CompareExchange",
    "CounterGroups",
    "Attachments",
    "TimeSeries",
    "TimeSeriesDeletedRanges",
    "Subscriptions",
    "Tombstones",
    "CompareExchangeTombstones",
];

export const allSettingTokens: DatabaseRecordItemType[] = [
    "Settings",
    "ConflictSolverConfig",
    "Client",
    "Revisions",
    "Refresh",
    "Expiration",
    "DocumentsCompression",
    "SchemaValidation",
    "DataArchival",
    "TimeSeries",
    "Sorters",
    "Analyzers",
    "PostgreSQLIntegration",
];

export const allOngoingTaskTokens: DatabaseRecordItemType[] = [
    "PeriodicBackups",
    "ExternalReplications",
    "RavenEtls",
    "SqlEtls",
    "SnowflakeEtls",
    "OlapEtls",
    "ElasticSearchEtls",
    "QueueEtls",
    "HubPullReplications",
    "SinkPullReplications",
    "EmbeddingsGenerations",
    "GenAiEtls",
    "CdcSinks",
    "AiAgents",
    "RemoteAttachments",
];

export const allConnectionStringTokens: DatabaseRecordItemType[] = [
    "RavenConnectionStrings",
    "SqlConnectionStrings",
    "SnowflakeConnectionStrings",
    "OlapConnectionStrings",
    "ElasticSearchConnectionStrings",
    "QueueConnectionStrings",
    "AiConnectionStrings",
];

export const adminDefaultDto: ImportOptions = {
    IncludeExpired: true,
    IncludeArtificial: false,
    IncludeArchived: true,
    TransformScript: "",
    RemoveAnalyzers: false,
    EncryptionKey: undefined,
    OperateOnTypes: adminDefaultOperateOnTypes.join(",") as DatabaseItemType,
    OperateOnDatabaseRecordTypes: "None",
    Collections: null,
    MaxReadOpsPerSecond: null,
} as ImportOptions;

export const without = <T>(list: T[], ...excluded: T[]) => list.filter((x) => !excluded.includes(x));

export const operateOnTypesOf = (dto: ImportOptions) => dto.OperateOnTypes.split(",");
export const recordTypesOf = (dto: ImportOptions) => dto.OperateOnDatabaseRecordTypes.split(",");
