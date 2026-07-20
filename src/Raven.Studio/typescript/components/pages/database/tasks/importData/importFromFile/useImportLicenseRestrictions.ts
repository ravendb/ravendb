import { useMemo } from "react";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { LicenseBadgeText } from "components/common/LicenseRestrictedBadge";
import { DatabaseSettingKey, OngoingTaskKey } from "./importFromFileValidation";

type LicenseStatusKey = keyof Raven.Server.Commercial.LicenseStatus;

export interface RestrictedImportFeature {
    settingKey: DatabaseSettingKey;
    label: string;
    // lowest license tier that includes the feature (per the availability matrix in LicenseDetails.tsx)
    licenseRequired: LicenseBadgeText;
}

export interface RestrictedImportOngoingTask {
    taskKey: OngoingTaskKey;
    label: string;
    licenseRequired: LicenseBadgeText;
}

const settingFlags: { settingKey: DatabaseSettingKey; label: string; flag: LicenseStatusKey; licenseRequired: LicenseBadgeText }[] = [
    { settingKey: "documentsCompression", label: "Documents Compression", flag: "HasDocumentsCompression", licenseRequired: "Enterprise" },
    { settingKey: "dataArchival", label: "Data Archival", flag: "HasDataArchival", licenseRequired: "Enterprise" },
    { settingKey: "timeSeries", label: "Time Series Configuration", flag: "HasTimeSeriesRollupsAndRetention", licenseRequired: "Professional +" },
    { settingKey: "postgreSqlIntegration", label: "PostgreSQL Integration", flag: "HasPostgreSqlIntegration", licenseRequired: "Enterprise" },
    { settingKey: "client", label: "Client Configuration", flag: "HasClientConfiguration", licenseRequired: "Professional +" },
];

// aiAgents and connection strings have no dedicated license flag - not gated
const ongoingTaskFlags: { taskKey: OngoingTaskKey; label: string; flag: LicenseStatusKey; licenseRequired: LicenseBadgeText }[] = [
    { taskKey: "periodicBackups", label: "Periodic Backups", flag: "HasPeriodicBackup", licenseRequired: "Professional +" },
    { taskKey: "externalReplications", label: "External Replications", flag: "HasExternalReplication", licenseRequired: "Professional +" },
    { taskKey: "ravenEtls", label: "RavenDB ETLs", flag: "HasRavenEtl", licenseRequired: "Professional +" },
    { taskKey: "sqlEtls", label: "SQL ETLs", flag: "HasSqlEtl", licenseRequired: "Professional +" },
    { taskKey: "snowflakeEtls", label: "Snowflake ETLs", flag: "HasSnowflakeEtl", licenseRequired: "Enterprise" },
    { taskKey: "olapEtls", label: "OLAP ETLs", flag: "HasOlapEtl", licenseRequired: "Enterprise" },
    { taskKey: "elasticSearchEtls", label: "Elasticsearch ETLs", flag: "HasElasticSearchEtl", licenseRequired: "Enterprise" },
    { taskKey: "queueEtls", label: "Queue ETLs", flag: "HasQueueEtl", licenseRequired: "Enterprise" },
    { taskKey: "hubReplications", label: "Replication Hubs", flag: "HasPullReplicationAsHub", licenseRequired: "Enterprise" },
    { taskKey: "sinkReplications", label: "Replication Sinks", flag: "HasPullReplicationAsSink", licenseRequired: "Professional +" },
    { taskKey: "embeddingsGeneration", label: "Embeddings Generation", flag: "HasEmbeddingsGeneration", licenseRequired: "Enterprise" },
    { taskKey: "genAi", label: "GenAI", flag: "HasGenAi", licenseRequired: "Enterprise AI" },
    { taskKey: "cdcSinks", label: "CDC Sinks", flag: "HasCdcSink", licenseRequired: "Enterprise" },
    { taskKey: "remoteAttachments", label: "Remote Attachments", flag: "HasRemoteAttachments", licenseRequired: "Enterprise" },
];

function getRestrictionTooltipText(label: string): string {
    return `Data created with ${label} won't be imported - this feature isn't included in your license`;
}

export function useImportLicenseRestrictions(): {
    restrictedFeatures: RestrictedImportFeature[];
    restrictedOngoingTasks: RestrictedImportOngoingTask[];
    // combined list for the license alert chips next to the file input
    allRestrictedItems: { key: string; label: string; licenseRequired: LicenseBadgeText }[];
    isSettingRestricted: (key: DatabaseSettingKey) => boolean;
    getRestrictionTooltip: (key: DatabaseSettingKey) => string | null;
    getLicenseRequired: (key: DatabaseSettingKey) => LicenseBadgeText | null;
    isOngoingTaskRestricted: (key: OngoingTaskKey) => boolean;
    getOngoingTaskRestrictionTooltip: (key: OngoingTaskKey) => string | null;
    getOngoingTaskLicenseRequired: (key: OngoingTaskKey) => LicenseBadgeText | null;
} {
    const licenseStatus = useAppSelector(licenseSelectors.status);

    return useMemo(() => {
        const restrictedFeatures: RestrictedImportFeature[] = settingFlags
            .filter(({ flag }) => !licenseStatus?.[flag])
            .map(({ settingKey, label, licenseRequired }) => ({ settingKey, label, licenseRequired }));

        const restrictedOngoingTasks: RestrictedImportOngoingTask[] = ongoingTaskFlags
            .filter(({ flag }) => !licenseStatus?.[flag])
            .map(({ taskKey, label, licenseRequired }) => ({ taskKey, label, licenseRequired }));

        const allRestrictedItems = [
            ...restrictedFeatures.map((x) => ({ key: `setting-${x.settingKey}`, label: x.label, licenseRequired: x.licenseRequired })),
            ...restrictedOngoingTasks.map((x) => ({ key: `task-${x.taskKey}`, label: x.label, licenseRequired: x.licenseRequired })),
        ];

        const isSettingRestricted = (key: DatabaseSettingKey) =>
            restrictedFeatures.some((feature) => feature.settingKey === key);

        const getRestrictionTooltip = (key: DatabaseSettingKey) => {
            const feature = restrictedFeatures.find((f) => f.settingKey === key);
            return feature ? getRestrictionTooltipText(feature.label) : null;
        };

        const getLicenseRequired = (key: DatabaseSettingKey) =>
            restrictedFeatures.find((f) => f.settingKey === key)?.licenseRequired ?? null;

        const isOngoingTaskRestricted = (key: OngoingTaskKey) =>
            restrictedOngoingTasks.some((task) => task.taskKey === key);

        const getOngoingTaskRestrictionTooltip = (key: OngoingTaskKey) => {
            const task = restrictedOngoingTasks.find((t) => t.taskKey === key);
            return task ? getRestrictionTooltipText(task.label) : null;
        };

        const getOngoingTaskLicenseRequired = (key: OngoingTaskKey) =>
            restrictedOngoingTasks.find((t) => t.taskKey === key)?.licenseRequired ?? null;

        return {
            restrictedFeatures,
            restrictedOngoingTasks,
            allRestrictedItems,
            isSettingRestricted,
            getRestrictionTooltip,
            getLicenseRequired,
            isOngoingTaskRestricted,
            getOngoingTaskRestrictionTooltip,
            getOngoingTaskLicenseRequired,
        };
    }, [licenseStatus]);
}
