import { LicenseBadgeText } from "components/common/LicenseRestrictedBadge";
import { ConnectionStringKey, DatabaseSettingKey, OngoingTaskKey } from "./importFromFileValidation";
import { DocumentToggleKey } from "components/pages/database/tasks/importData/importFromFile/useImportRestrictions";

type LicenseStatusKey = keyof LicenseStatus;

/**
 * Why an import entry is unavailable. The three reasons are independent and need different UI:
 * a license gap shows a license badge and can be lifted by upgrading, sharding and access level
 * cannot - so they get a plain explanation instead of a badge.
 */
export type ImportRestrictionReason = "license" | "sharding" | "access";

export interface ImportRestriction {
    reason: ImportRestrictionReason;
    /** Human-readable feature name, used in chips and tooltips. */
    label: string;
    /** Only set when reason === "license". */
    licenseRequired?: LicenseBadgeText;
    tooltip: string;
}

/**
 * Single source of truth for every gated import entry.
 *
 * `licenseFlags` holds the license flags that enable the entry. An entry counts as
 * license-restricted only when EVERY listed flag is missing: some entries (AI / queue connection
 * strings) back more than one feature, so a license that has any one of them can still make use
 * of the entry. Single-flag entries behave exactly as before.
 *
 * `isShardingSupported` and `accessRequired` mirror the ongoing-task cards in
 * tasks/shared/shared.tsx - keep the two in sync when a task gains sharding support.
 */
interface ImportRestrictionRule {
    label: string;
    licenseFlags?: LicenseStatusKey[];
    licenseRequired?: LicenseBadgeText;
    /**
     * Only meaningful for ongoing tasks, where support is opt-in (an omitted flag means the task
     * cannot run on a sharded database). Entries that are sharding-agnostic - database settings and
     * connection strings, which are database-record data rather than running tasks - set
     * `isShardingChecked: false` on their group instead of repeating `isShardingSupported: true`.
     */
    isShardingSupported?: boolean;
    accessRequired?: accessLevel;
}

export const documentToggleRules: Partial<Record<DocumentToggleKey, ImportRestrictionRule>> = {
    isIncludeArchivedDocuments: {
        label: "Archived Documents",
        licenseFlags: ["HasDataArchival"],
        licenseRequired: "Enterprise",
    },
};

export const databaseSettingRules: Partial<Record<DatabaseSettingKey, ImportRestrictionRule>> = {
    documentsCompression: {
        label: "Documents Compression",
        licenseFlags: ["HasDocumentsCompression"],
        licenseRequired: "Enterprise",
    },
    dataArchival: { label: "Data Archival", licenseFlags: ["HasDataArchival"], licenseRequired: "Enterprise" },
    timeSeries: {
        label: "Time Series Configuration",
        licenseFlags: ["HasTimeSeriesRollupsAndRetention"],
        licenseRequired: "Professional +",
    },
    postgreSqlIntegration: {
        label: "PostgreSQL Integration",
        licenseFlags: ["HasPostgreSqlIntegration"],
        licenseRequired: "Enterprise",
    },
    client: {
        label: "Client Configuration",
        licenseFlags: ["HasClientConfiguration"],
        licenseRequired: "Professional +",
    },
    schemaValidation: {
        label: "Document Schema",
        licenseFlags: ["HasSchemaValidation"],
        licenseRequired: "Professional +",
    },
};

export const ongoingTaskRules: Partial<Record<OngoingTaskKey, ImportRestrictionRule>> = {
    periodicBackups: {
        label: "Periodic Backups",
        licenseFlags: ["HasPeriodicBackup"],
        licenseRequired: "Professional +",
        isShardingSupported: true,
        accessRequired: "DatabaseAdmin",
    },
    externalReplications: {
        label: "External Replications",
        licenseFlags: ["HasExternalReplication"],
        licenseRequired: "Professional +",
        isShardingSupported: true,
        accessRequired: "DatabaseAdmin",
    },
    ravenEtls: {
        label: "RavenDB ETLs",
        licenseFlags: ["HasRavenEtl"],
        licenseRequired: "Professional +",
        isShardingSupported: true,
        accessRequired: "DatabaseAdmin",
    },
    sqlEtls: {
        label: "SQL ETLs",
        licenseFlags: ["HasSqlEtl"],
        licenseRequired: "Professional +",
        isShardingSupported: true,
        accessRequired: "DatabaseAdmin",
    },
    snowflakeEtls: {
        label: "Snowflake ETLs",
        licenseFlags: ["HasSnowflakeEtl"],
        licenseRequired: "Enterprise",
        isShardingSupported: true,
        accessRequired: "DatabaseAdmin",
    },
    olapEtls: {
        label: "OLAP ETLs",
        licenseFlags: ["HasOlapEtl"],
        licenseRequired: "Enterprise",
        isShardingSupported: true,
        accessRequired: "DatabaseAdmin",
    },
    elasticSearchEtls: {
        label: "Elasticsearch ETLs",
        licenseFlags: ["HasElasticSearchEtl"],
        licenseRequired: "Enterprise",
        isShardingSupported: true,
        accessRequired: "DatabaseAdmin",
    },
    // Queue ETLs cover Kafka, RabbitMQ, Azure Queue Storage and Amazon SQS - none of which is
    // sharding-supported in tasks/shared/shared.tsx
    queueEtls: {
        label: "Queue ETLs",
        licenseFlags: ["HasQueueEtl"],
        licenseRequired: "Enterprise",
        accessRequired: "DatabaseAdmin",
    },
    hubReplications: {
        label: "Replication Hubs",
        licenseFlags: ["HasPullReplicationAsHub"],
        licenseRequired: "Enterprise",
        accessRequired: "DatabaseAdmin",
    },
    sinkReplications: {
        label: "Replication Sinks",
        licenseFlags: ["HasPullReplicationAsSink"],
        licenseRequired: "Professional +",
        accessRequired: "DatabaseAdmin",
    },
    embeddingsGeneration: {
        label: "Embeddings Generation",
        licenseFlags: ["HasEmbeddingsGeneration"],
        licenseRequired: "Enterprise",
        isShardingSupported: true,
        accessRequired: "DatabaseAdmin",
    },
    genAi: {
        label: "GenAI",
        licenseFlags: ["HasGenAi"],
        licenseRequired: "Enterprise AI",
        accessRequired: "DatabaseAdmin",
    },
    aiAgents: {
        label: "AI Agents",
        licenseFlags: ["HasAiAgent"],
        licenseRequired: "Enterprise AI",
        accessRequired: "DatabaseAdmin",
    },
    cdcSinks: {
        label: "CDC Sinks",
        licenseFlags: ["HasCdcSink"],
        licenseRequired: "Enterprise",
        accessRequired: "DatabaseAdmin",
    },
    remoteAttachments: {
        label: "Remote Attachments",
        licenseFlags: ["HasRemoteAttachments"],
        licenseRequired: "Enterprise",
        accessRequired: "DatabaseAdmin",
    },
};

export const connectionStringRules: Partial<Record<ConnectionStringKey, ImportRestrictionRule>> = {
    ravenConnectionStrings: {
        label: "RavenDB Connection Strings",
        licenseFlags: ["HasRavenEtl"],
        licenseRequired: "Professional +",
        accessRequired: "DatabaseAdmin",
    },
    sqlConnectionStrings: {
        label: "SQL Connection Strings",
        licenseFlags: ["HasSqlEtl"],
        licenseRequired: "Professional +",
        accessRequired: "DatabaseAdmin",
    },
    snowflakeConnectionStrings: {
        label: "Snowflake Connection Strings",
        licenseFlags: ["HasSnowflakeEtl"],
        licenseRequired: "Enterprise",
        accessRequired: "DatabaseAdmin",
    },
    olapConnectionStrings: {
        label: "OLAP Connection Strings",
        licenseFlags: ["HasOlapEtl"],
        licenseRequired: "Enterprise",
        accessRequired: "DatabaseAdmin",
    },
    elasticSearchConnectionStrings: {
        label: "Elasticsearch Connection Strings",
        licenseFlags: ["HasElasticSearchEtl"],
        licenseRequired: "Enterprise",
        accessRequired: "DatabaseAdmin",
    },
    queueConnectionStrings: {
        label: "Queue Connection Strings",
        licenseFlags: ["HasQueueEtl", "HasQueueSink"],
        licenseRequired: "Enterprise",
        accessRequired: "DatabaseAdmin",
    },
    aiConnectionStrings: {
        label: "AI Connection Strings",
        licenseFlags: ["HasGenAi", "HasAiAgent", "HasEmbeddingsGeneration"],
        licenseRequired: "Enterprise AI",
        accessRequired: "DatabaseAdmin",
    },
};

export function getLicenseRestrictionTooltip(label: string) {
    return `Data created with ${label} won't be imported - this feature isn't included in your license`;
}

export function getShardingRestrictionTooltip(label: string) {
    return `${label} are not supported for sharded databases and won't be imported`;
}

export function getAccessRestrictionTooltip(label: string) {
    return `Your certificate doesn't grant sufficient permissions to import ${label}`;
}

/**
 * Resolves a rule against the current license / sharding / access state.
 * License is reported first because it is the only reason the user can act on.
 */
export function resolveRestriction(
    rule: ImportRestrictionRule | undefined,
    context: {
        licenseStatus: LicenseStatus | null;
        isSharded: boolean;
        canHandleOperation: (requiredAccess: accessLevel) => boolean;
        /** Set to false for groups where sharding is irrelevant (settings, connection strings). */
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
            licenseRequired: rule.licenseRequired,
            tooltip: getLicenseRestrictionTooltip(rule.label),
        };
    }

    if (isShardingChecked && isSharded && !rule.isShardingSupported) {
        return {
            reason: "sharding",
            label: rule.label,
            tooltip: getShardingRestrictionTooltip(rule.label),
        };
    }

    if (rule.accessRequired && !canHandleOperation(rule.accessRequired)) {
        return {
            reason: "access",
            label: rule.label,
            tooltip: getAccessRestrictionTooltip(rule.label),
        };
    }

    return null;
}
