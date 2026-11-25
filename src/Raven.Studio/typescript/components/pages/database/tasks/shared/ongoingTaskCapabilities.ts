import { LicenseBadgeText } from "components/common/LicenseRestrictedBadge";
import type { CloudOptionalFeature } from "components/common/FeatureAvailabilitySummary";

type LicenseStatusKey = keyof LicenseStatus;

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

export interface OngoingTaskCapabilities {
    licenseFlags: LicenseStatusKey[];
    licenseBadge?: LicenseBadgeText;
    cloudOptionalFeature?: CloudOptionalFeature;
    isShardingSupported?: boolean;
    accessRequired: databaseAccessLevel;
}

export const ongoingTaskCapabilities: Record<OngoingTaskTarget, OngoingTaskCapabilities> = {
    GenAi: {
        licenseFlags: ["HasGenAi"],
        licenseBadge: "Enterprise AI",
        cloudOptionalFeature: "ai",
        accessRequired: "DatabaseAdmin",
    },
    AiAgent: {
        licenseFlags: ["HasAiAgent"],
        licenseBadge: "Enterprise AI",
        cloudOptionalFeature: "ai",
        accessRequired: "DatabaseAdmin",
    },
    EmbeddingsGeneration: {
        licenseFlags: ["HasEmbeddingsGeneration"],
        licenseBadge: "Enterprise",
        cloudOptionalFeature: "ai",
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
        cloudOptionalFeature: "queueEtl",
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
        cloudOptionalFeature: "snowflakeEtl",
        isShardingSupported: true,
        accessRequired: "DatabaseAdmin",
    },
    OlapETL: {
        licenseFlags: ["HasOlapEtl"],
        licenseBadge: "Enterprise",
        cloudOptionalFeature: "olapEtl",
        isShardingSupported: true,
        accessRequired: "DatabaseAdmin",
    },
    RabbitMqETL: {
        licenseFlags: ["HasQueueEtl"],
        licenseBadge: "Enterprise",
        cloudOptionalFeature: "queueEtl",
        accessRequired: "DatabaseAdmin",
    },
    AzureQueueStorageETL: {
        licenseFlags: ["HasQueueEtl"],
        licenseBadge: "Enterprise",
        cloudOptionalFeature: "queueEtl",
        accessRequired: "DatabaseAdmin",
    },
    AmazonSqsETL: {
        licenseFlags: ["HasQueueEtl"],
        licenseBadge: "Enterprise",
        cloudOptionalFeature: "queueEtl",
        accessRequired: "DatabaseAdmin",
    },
    KafkaSink: {
        licenseFlags: ["HasQueueSink"],
        licenseBadge: "Enterprise",
        cloudOptionalFeature: "queueSink",
        accessRequired: "DatabaseAdmin",
    },
    RabbitMqSink: {
        licenseFlags: ["HasQueueSink"],
        licenseBadge: "Enterprise",
        cloudOptionalFeature: "queueSink",
        accessRequired: "DatabaseAdmin",
    },
    AzureServiceBusSink: {
        licenseFlags: ["HasQueueSink"],
        licenseBadge: "Enterprise",
        cloudOptionalFeature: "queueSink",
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
        cloudOptionalFeature: "dataGovernance",
        accessRequired: "DatabaseAdmin",
    },
};

export function mergeCapabilities(targets: OngoingTaskTarget[]): OngoingTaskCapabilities {
    const entries = targets.map((target) => ongoingTaskCapabilities[target]);

    return {
        licenseFlags: [...new Set(entries.flatMap((x) => x.licenseFlags))],
        licenseBadge: entries[0].licenseBadge,
        cloudOptionalFeature: entries[0].cloudOptionalFeature,
        isShardingSupported: entries.every((x) => x.isShardingSupported),
        accessRequired: entries[0].accessRequired,
    };
}
