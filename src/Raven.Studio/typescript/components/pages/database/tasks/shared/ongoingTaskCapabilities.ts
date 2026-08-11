import { LicenseBadgeText } from "components/common/LicenseRestrictedBadge";

type LicenseStatusKey = keyof LicenseStatus;

/**
 * Every ongoing-task type the studio can create, keyed by its navigation-card target.
 */
export type OngoingTaskTarget =
    | "GenAi"
    | "AiAgent"
    | "EmbeddingsGeneration"
    | "ExternalReplication"
    | "ReplicationHub"
    | "ReplicationSink"
    | "PeriodicBackup"
    | "Subscription"
    | "RavenETL"
    | "ElasticSearchETL"
    | "KafkaETL"
    | "SqlETL"
    | "SnowflakeETL"
    | "OlapETL"
    | "RabbitMqETL"
    | "AzureQueueStorageETL"
    | "AmazonSqsETL"
    | "KafkaSink"
    | "RabbitMqSink"
    | "AzureServiceBusSink"
    | "CdcSink"
    | "RemoteAttachments";

/**
 * What a licence, a sharded database and a certificate's access level allow for one task type.
 *
 * Single source of truth for the two places that gate on it: the "Add new ongoing task" cards
 * (tasks/shared/shared.tsx) and the import-from-file restrictions (importData/importFromFile).
 */
export interface OngoingTaskCapabilities {
    /**
     * Licence flags that enable the task. It counts as licence-restricted only when EVERY listed
     * flag is missing - a few entries back more than one feature, so a licence holding any one of
     * them still enables the task.
     */
    licenseFlags: LicenseStatusKey[];
    /** Omitted for tasks that every licence includes. */
    licenseBadge?: LicenseBadgeText;
    /** Omitted means the task cannot run on a sharded database. */
    isShardingSupported?: boolean;
    accessRequired: databaseAccessLevel;
}

export const ongoingTaskCapabilities: Record<OngoingTaskTarget, OngoingTaskCapabilities> = {
    GenAi: {
        licenseFlags: ["HasGenAi"],
        licenseBadge: "Enterprise AI",
        accessRequired: "DatabaseAdmin",
    },
    AiAgent: {
        licenseFlags: ["HasAiAgent"],
        licenseBadge: "Enterprise AI",
        accessRequired: "DatabaseAdmin",
    },
    EmbeddingsGeneration: {
        licenseFlags: ["HasEmbeddingsGeneration"],
        licenseBadge: "Enterprise",
        isShardingSupported: true,
        accessRequired: "DatabaseAdmin",
    },
    ExternalReplication: {
        licenseFlags: ["HasExternalReplication"],
        licenseBadge: "Professional +",
        isShardingSupported: true,
        accessRequired: "DatabaseAdmin",
    },
    ReplicationHub: {
        licenseFlags: ["HasPullReplicationAsHub"],
        licenseBadge: "Enterprise",
        accessRequired: "DatabaseAdmin",
    },
    ReplicationSink: {
        licenseFlags: ["HasPullReplicationAsSink"],
        licenseBadge: "Professional +",
        accessRequired: "DatabaseAdmin",
    },
    PeriodicBackup: {
        licenseFlags: ["HasPeriodicBackup"],
        licenseBadge: "Professional +",
        isShardingSupported: true,
        accessRequired: "DatabaseAdmin",
    },
    Subscription: {
        licenseFlags: [],
        isShardingSupported: true,
        accessRequired: "DatabaseReadWrite",
    },
    RavenETL: {
        licenseFlags: ["HasRavenEtl"],
        licenseBadge: "Professional +",
        isShardingSupported: true,
        accessRequired: "DatabaseAdmin",
    },
    ElasticSearchETL: {
        licenseFlags: ["HasElasticSearchEtl"],
        licenseBadge: "Enterprise",
        isShardingSupported: true,
        accessRequired: "DatabaseAdmin",
    },
    KafkaETL: {
        licenseFlags: ["HasQueueEtl"],
        licenseBadge: "Enterprise",
        accessRequired: "DatabaseAdmin",
    },
    SqlETL: {
        licenseFlags: ["HasSqlEtl"],
        licenseBadge: "Professional +",
        isShardingSupported: true,
        accessRequired: "DatabaseAdmin",
    },
    SnowflakeETL: {
        licenseFlags: ["HasSnowflakeEtl"],
        licenseBadge: "Enterprise",
        isShardingSupported: true,
        accessRequired: "DatabaseAdmin",
    },
    OlapETL: {
        licenseFlags: ["HasOlapEtl"],
        licenseBadge: "Enterprise",
        isShardingSupported: true,
        accessRequired: "DatabaseAdmin",
    },
    RabbitMqETL: {
        licenseFlags: ["HasQueueEtl"],
        licenseBadge: "Enterprise",
        accessRequired: "DatabaseAdmin",
    },
    AzureQueueStorageETL: {
        licenseFlags: ["HasQueueEtl"],
        licenseBadge: "Enterprise",
        accessRequired: "DatabaseAdmin",
    },
    AmazonSqsETL: {
        licenseFlags: ["HasQueueEtl"],
        licenseBadge: "Enterprise",
        accessRequired: "DatabaseAdmin",
    },
    KafkaSink: {
        licenseFlags: ["HasQueueSink"],
        licenseBadge: "Enterprise",
        accessRequired: "DatabaseAdmin",
    },
    RabbitMqSink: {
        licenseFlags: ["HasQueueSink"],
        licenseBadge: "Enterprise",
        accessRequired: "DatabaseAdmin",
    },
    AzureServiceBusSink: {
        licenseFlags: ["HasQueueSink"],
        licenseBadge: "Enterprise",
        accessRequired: "DatabaseAdmin",
    },
    CdcSink: {
        licenseFlags: ["HasCdcSink"],
        licenseBadge: "Enterprise",
        accessRequired: "DatabaseAdmin",
    },
    RemoteAttachments: {
        licenseFlags: ["HasRemoteAttachments"],
        licenseBadge: "Enterprise",
        accessRequired: "DatabaseAdmin",
    },
};

/**
 * Merges the capabilities of several task types into one entry - the import view gates whole
 * groups (e.g. one "Queue ETLs" row covering Kafka, RabbitMQ, Azure Queue Storage and Amazon SQS).
 * A group is licence-restricted only when every member is, and sharded only when every member is
 * sharding-supported.
 */
export function mergeCapabilities(targets: OngoingTaskTarget[]): OngoingTaskCapabilities {
    const entries = targets.map((target) => ongoingTaskCapabilities[target]);

    return {
        licenseFlags: [...new Set(entries.flatMap((x) => x.licenseFlags))],
        licenseBadge: entries[0].licenseBadge,
        isShardingSupported: entries.every((x) => x.isShardingSupported),
        accessRequired: entries[0].accessRequired,
    };
}
