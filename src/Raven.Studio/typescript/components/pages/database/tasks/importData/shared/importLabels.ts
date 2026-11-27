import IconName from "typings/server/icons";
import {
    ConnectionStringKey,
    DatabaseSettingKey,
    OngoingTaskKey,
} from "components/pages/database/tasks/importData/shared/importOptionsValidation";

export const databaseSettingIcons: Record<DatabaseSettingKey, IconName> = {
    settings: "database-settings",
    conflictSolverConfig: "conflicts-resolution",
    client: "database-client-configuration",
    revisions: "revisions",
    refresh: "expos-refresh",
    expiration: "document-expiration",
    documentsCompression: "documents-compression",
    schemaValidation: "document-schema",
    dataArchival: "data-archival",
    timeSeries: "timeseries-settings",
    sorters: "custom-sorters",
    analyzers: "custom-analyzers",
    postgreSqlIntegration: "postgresql",
};

export const ongoingTaskIcons: Record<OngoingTaskKey, IconName> = {
    periodicBackups: "periodic-backup",
    externalReplications: "external-replication",
    ravenEtls: "ravendb-etl",
    sqlEtls: "sql-etl",
    snowflakeEtls: "snowflake-etl",
    olapEtls: "olap-etl",
    elasticSearchEtls: "elastic-search-etl",
    queueEtls: "kafka-etl",
    hubReplications: "pull-replication-hub",
    sinkReplications: "pull-replication-agent",
    embeddingsGeneration: "ai-etl",
    genAi: "genai",
    cdcSinks: "sql-etl",
    aiAgents: "ai-agents",
    remoteAttachments: "remote-attachment",
};

export const connectionStringIcons: Record<ConnectionStringKey, IconName> = {
    ravenConnectionStrings: "ravendb-etl",
    sqlConnectionStrings: "sql-etl",
    snowflakeConnectionStrings: "snowflake-etl",
    olapConnectionStrings: "olap-etl",
    elasticSearchConnectionStrings: "elastic-search-etl",
    queueConnectionStrings: "kafka-etl",
    aiConnectionStrings: "ai",
};
