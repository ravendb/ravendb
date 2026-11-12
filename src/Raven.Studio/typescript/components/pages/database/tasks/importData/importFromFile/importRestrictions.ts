import { LicenseBadgeText } from "components/common/LicenseRestrictedBadge";
import {
    ConnectionStringKey,
    connectionStringKeys,
    DatabaseSettingKey,
    DocumentToggleKey,
    OngoingTaskKey,
    ongoingTaskKeys,
} from "./importFromFileValidation";
import {
    connectionStringIcons,
    connectionStringLabels,
    databaseSettingIcons,
    databaseSettingLabels,
    ongoingTaskIcons,
    ongoingTaskLabels,
} from "./importFromFileLabels";
import IconName from "../../../../../../../typings/server/icons";
import { mergeCapabilities, OngoingTaskTarget } from "components/pages/database/tasks/shared/ongoingTaskCapabilities";

type LicenseStatusKey = keyof LicenseStatus;

export type ImportRestrictionReason = "license" | "sharding" | "access";

export interface ImportRestriction {
    reason: ImportRestrictionReason;
    label: string;
    icon: IconName;
    licenseRequired?: LicenseBadgeText;
    tooltip: string;
}

interface ImportRestrictionRule {
    label: string;
    icon: IconName;
    licenseFlags?: LicenseStatusKey[];
    licenseRequired?: LicenseBadgeText;
    isShardingSupported?: boolean;
    accessRequired?: accessLevel;
}

export const documentToggleRules: Partial<Record<DocumentToggleKey, ImportRestrictionRule>> = {
    isIncludeArchivedDocuments: {
        label: "Archived Documents",
        icon: "data-archival",
        licenseFlags: ["HasDataArchival"],
        licenseRequired: "Enterprise",
    },
};

export const databaseSettingRules: Partial<Record<DatabaseSettingKey, ImportRestrictionRule>> = {
    revisions: {
        label: databaseSettingLabels.revisions,
        icon: databaseSettingIcons.revisions,
        licenseFlags: ["CanSetupDefaultRevisionsConfiguration"],
        licenseRequired: "Professional +",
    },
    documentsCompression: {
        label: databaseSettingLabels.documentsCompression,
        icon: databaseSettingIcons.documentsCompression,
        licenseFlags: ["HasDocumentsCompression"],
        licenseRequired: "Enterprise",
    },
    dataArchival: {
        label: databaseSettingLabels.dataArchival,
        icon: databaseSettingIcons.dataArchival,
        licenseFlags: ["HasDataArchival"],
        licenseRequired: "Enterprise",
    },
    timeSeries: {
        label: databaseSettingLabels.timeSeries,
        icon: databaseSettingIcons.timeSeries,
        licenseFlags: ["HasTimeSeriesRollupsAndRetention"],
        licenseRequired: "Professional +",
    },
    postgreSqlIntegration: {
        label: databaseSettingLabels.postgreSqlIntegration,
        icon: databaseSettingIcons.postgreSqlIntegration,
        licenseFlags: ["HasPostgreSqlIntegration"],
        licenseRequired: "Enterprise",
    },
    client: {
        label: databaseSettingLabels.client,
        icon: databaseSettingIcons.client,
        licenseFlags: ["HasClientConfiguration"],
        licenseRequired: "Professional +",
    },
    schemaValidation: {
        label: databaseSettingLabels.schemaValidation,
        icon: databaseSettingIcons.schemaValidation,
        licenseFlags: ["HasSchemaValidation"],
        licenseRequired: "Professional +",
    },
};

const ongoingTaskTargets: Record<OngoingTaskKey, OngoingTaskTarget[]> = {
    periodicBackups: ["PeriodicBackup"],
    externalReplications: ["ExternalReplication"],
    ravenEtls: ["RavenETL"],
    sqlEtls: ["SqlETL"],
    snowflakeEtls: ["SnowflakeETL"],
    olapEtls: ["OlapETL"],
    elasticSearchEtls: ["ElasticSearchETL"],
    queueEtls: ["KafkaETL", "RabbitMqETL", "AzureQueueStorageETL", "AmazonSqsETL"],
    hubReplications: ["ReplicationHub"],
    sinkReplications: ["ReplicationSink"],
    embeddingsGeneration: ["EmbeddingsGeneration"],
    genAi: ["GenAi"],
    aiAgents: ["AiAgent"],
    cdcSinks: ["CdcSink"],
    remoteAttachments: ["RemoteAttachments"],
};

export const ongoingTaskRules: Partial<Record<OngoingTaskKey, ImportRestrictionRule>> = Object.fromEntries(
    ongoingTaskKeys.map((key) => {
        const { licenseFlags, licenseBadge, isShardingSupported, accessRequired } = mergeCapabilities(
            ongoingTaskTargets[key]
        );

        return [
            key,
            {
                label: ongoingTaskLabels[key],
                icon: ongoingTaskIcons[key],
                licenseFlags,
                licenseRequired: licenseBadge,
                isShardingSupported,
                accessRequired,
            } satisfies ImportRestrictionRule,
        ];
    })
);

const connectionStringTargets: Record<ConnectionStringKey, OngoingTaskTarget[]> = {
    ravenConnectionStrings: ["RavenETL"],
    sqlConnectionStrings: ["SqlETL"],
    snowflakeConnectionStrings: ["SnowflakeETL"],
    olapConnectionStrings: ["OlapETL"],
    elasticSearchConnectionStrings: ["ElasticSearchETL"],
    queueConnectionStrings: ["KafkaETL", "RabbitMqETL", "AzureQueueStorageETL", "AmazonSqsETL", "KafkaSink"],
    aiConnectionStrings: ["GenAi", "AiAgent", "EmbeddingsGeneration"],
};

export const connectionStringRules: Partial<Record<ConnectionStringKey, ImportRestrictionRule>> = Object.fromEntries(
    connectionStringKeys.map((key) => {
        const { licenseFlags, licenseBadge, accessRequired } = mergeCapabilities(connectionStringTargets[key]);

        return [
            key,
            {
                label: connectionStringLabels[key],
                icon: connectionStringIcons[key],
                licenseFlags,
                licenseRequired: licenseBadge,
                accessRequired,
            } satisfies ImportRestrictionRule,
        ];
    })
);

export function getLicenseRestrictionTooltip(label: string) {
    return `Data created with ${label} won't be imported - this feature isn't included in your license`;
}

export function getShardingRestrictionTooltip(label: string) {
    return `${label} are not supported for sharded databases and won't be imported`;
}

export function getAccessRestrictionTooltip(label: string) {
    return `Your certificate doesn't grant sufficient permissions to import ${label}`;
}

export function resolveRestriction(
    rule: ImportRestrictionRule | undefined,
    context: {
        licenseStatus: LicenseStatus | null;
        isSharded: boolean;
        canHandleOperation: (requiredAccess: accessLevel) => boolean;
        isShardingChecked?: boolean;
    }
): ImportRestriction | null {
    if (!rule) {
        return null;
    }

    const { licenseStatus, isSharded, canHandleOperation, isShardingChecked = true } = context;

    const isLicenseRestricted =
        !!rule.licenseFlags?.length && rule.licenseFlags.every((flag) => !licenseStatus?.[flag]);

    if (isLicenseRestricted) {
        return {
            reason: "license",
            label: rule.label,
            icon: rule.icon,
            licenseRequired: rule.licenseRequired,
            tooltip: getLicenseRestrictionTooltip(rule.label),
        };
    }

    if (isShardingChecked && isSharded && !rule.isShardingSupported) {
        return {
            reason: "sharding",
            label: rule.label,
            icon: rule.icon,
            tooltip: getShardingRestrictionTooltip(rule.label),
        };
    }

    if (rule.accessRequired && !canHandleOperation(rule.accessRequired)) {
        return {
            reason: "access",
            label: rule.label,
            icon: rule.icon,
            tooltip: getAccessRestrictionTooltip(rule.label),
        };
    }

    return null;
}
