import IconName from "../../../../../../../typings/server/icons";
import { ConnectionStringKey, DatabaseSettingKey, OngoingTaskKey } from "./importFromFileValidation";

/**
 * Single source for the human-readable names of every import entry - used by the customize
 * switch rows and by the restriction rules (badges, tooltips), so the two can never drift apart.
 */

export const databaseSettingLabels: Record<DatabaseSettingKey, string> = {
    settings: "Settings",
    conflictSolverConfig: "Conflict Solver Configuration",
    client: "Client Configuration",
    revisions: "Revisions Configuration",
    refresh: "Document Refresh",
    expiration: "Document Expiration",
    documentsCompression: "Documents Compression",
    schemaValidation: "Document Schema",
    dataArchival: "Data Archival",
    timeSeries: "Time Series Configuration",
    sorters: "Custom Sorters",
    analyzers: "Custom Analyzers",
    postgreSqlIntegration: "PostgreSQL Integration",
};

export const ongoingTaskLabels: Record<OngoingTaskKey, string> = {
    periodicBackups: "Periodic Backups",
    externalReplications: "External Replications",
    ravenEtls: "RavenDB ETLs",
    sqlEtls: "SQL ETLs",
    snowflakeEtls: "Snowflake ETLs",
    olapEtls: "OLAP ETLs",
    elasticSearchEtls: "Elasticsearch ETLs",
    queueEtls: "Queue ETLs (Kafka, RabbitMQ, Azure Queue Storage, Amazon SQS)",
    hubReplications: "Replication Hubs",
    sinkReplications: "Replication Sinks",
    embeddingsGeneration: "Embeddings Generation",
    genAi: "GenAI",
    cdcSinks: "CDC Sinks",
    aiAgents: "AI Agents",
    remoteAttachments: "Remote Attachments",
};

export const connectionStringLabels: Record<ConnectionStringKey, string> = {
    ravenConnectionStrings: "RavenDB Connection Strings",
    sqlConnectionStrings: "SQL Connection Strings",
    snowflakeConnectionStrings: "Snowflake Connection Strings",
    olapConnectionStrings: "OLAP Connection Strings",
    elasticSearchConnectionStrings: "Elasticsearch Connection Strings",
    queueConnectionStrings: "Queue Connection Strings (Kafka, RabbitMQ, Azure Queue Storage, Amazon SQS)",
    aiConnectionStrings: "AI Connection Strings",
};

/**
 * Feature icons for the restriction chips, reusing the icon each feature already carries in its own
 * view (settings menu entries, "Add new ongoing task" cards) so an entry looks the same everywhere.
 */

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
