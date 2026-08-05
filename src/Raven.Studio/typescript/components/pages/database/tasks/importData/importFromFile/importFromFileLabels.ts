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
