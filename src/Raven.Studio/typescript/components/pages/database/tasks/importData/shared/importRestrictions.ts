import { LicenseBadgeText } from "components/common/LicenseRestrictedBadge";
import {
    ConnectionStringKey,
    connectionStringKeys,
    DatabaseSettingKey,
    DocumentToggleKey,
    OngoingTaskKey,
    ongoingTaskKeys,
} from "components/pages/database/tasks/importData/shared/importOptionsValidation";
import {
    connectionStringIcons,
    databaseSettingIcons,
    ongoingTaskIcons,
} from "components/pages/database/tasks/importData/shared/importLabels";
import IconName from "typings/server/icons";
import { mergeCapabilities, OngoingTaskTarget } from "components/pages/database/tasks/shared/ongoingTaskCapabilities";
import { createTranslator } from "common/i18n/i18n";
import { TranslationKey } from "common/i18n/resources";

const t = createTranslator("importData");

type LicenseStatusKey = keyof LicenseStatus;

export type ImportRestrictionReason = "license" | "sharding" | "access" | "sourceVersion";

export interface ImportRestriction {
    reason: ImportRestrictionReason;
    label: string;
    icon: IconName;
    licenseRequired?: LicenseBadgeText;
    tooltip: string;
}

type ImportItemLabelKey = Extract<
    TranslationKey<"importData">,
    `${"databaseSettings" | "ongoingTasks" | "connectionStrings"}.${string}` | "documents.archivedDocuments"
>;

interface ImportRestrictionRule {
    labelKey: ImportItemLabelKey;
    icon: IconName;
    licenseFlags?: LicenseStatusKey[];
    licenseRequired?: LicenseBadgeText;
    isShardingSupported?: boolean;
    accessRequired?: accessLevel;
}

export const documentToggleRules: Partial<Record<DocumentToggleKey, ImportRestrictionRule>> = {
    isIncludeArchivedDocuments: {
        labelKey: "documents.archivedDocuments",
        icon: "data-archival",
        licenseFlags: ["HasDataArchival"],
        licenseRequired: "Enterprise",
    },
};

export const databaseSettingRules: Partial<Record<DatabaseSettingKey, ImportRestrictionRule>> = {
    revisions: {
        labelKey: "databaseSettings.revisions",
        icon: databaseSettingIcons.revisions,
        licenseFlags: ["CanSetupDefaultRevisionsConfiguration"],
        licenseRequired: "Professional +",
    },
    documentsCompression: {
        labelKey: "databaseSettings.documentsCompression",
        icon: databaseSettingIcons.documentsCompression,
        licenseFlags: ["HasDocumentsCompression"],
        licenseRequired: "Enterprise",
    },
    dataArchival: {
        labelKey: "databaseSettings.dataArchival",
        icon: databaseSettingIcons.dataArchival,
        licenseFlags: ["HasDataArchival"],
        licenseRequired: "Enterprise",
    },
    timeSeries: {
        labelKey: "databaseSettings.timeSeries",
        icon: databaseSettingIcons.timeSeries,
        licenseFlags: ["HasTimeSeriesRollupsAndRetention"],
        licenseRequired: "Professional +",
    },
    postgreSqlIntegration: {
        labelKey: "databaseSettings.postgreSqlIntegration",
        icon: databaseSettingIcons.postgreSqlIntegration,
        licenseFlags: ["HasPostgreSqlIntegration"],
        licenseRequired: "Enterprise",
    },
    client: {
        labelKey: "databaseSettings.client",
        icon: databaseSettingIcons.client,
        licenseFlags: ["HasClientConfiguration"],
        licenseRequired: "Professional +",
    },
    schemaValidation: {
        labelKey: "databaseSettings.schemaValidation",
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
                labelKey: `ongoingTasks.${key}`,
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
                labelKey: `connectionStrings.${key}`,
                icon: connectionStringIcons[key],
                licenseFlags,
                licenseRequired: licenseBadge,
                accessRequired,
            } satisfies ImportRestrictionRule,
        ];
    })
);

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
    const label = t(rule.labelKey);

    const isLicenseRestricted =
        !!rule.licenseFlags?.length && rule.licenseFlags.every((flag) => !licenseStatus?.[flag]);

    if (isLicenseRestricted) {
        return {
            reason: "license",
            label,
            icon: rule.icon,
            licenseRequired: rule.licenseRequired,
            tooltip: t("restrictions.tooltip", { context: "license", label }),
        };
    }

    if (isShardingChecked && isSharded && !rule.isShardingSupported) {
        return {
            reason: "sharding",
            label,
            icon: rule.icon,
            tooltip: t("restrictions.tooltip", { context: "sharding", label }),
        };
    }

    if (rule.accessRequired && !canHandleOperation(rule.accessRequired)) {
        return {
            reason: "access",
            label,
            icon: rule.icon,
            tooltip: t("restrictions.tooltip", { context: "access", label }),
        };
    }

    return null;
}
