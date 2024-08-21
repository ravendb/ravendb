import moment from "moment";
import LicenseLimitsUsage = Raven.Server.Commercial.LicenseLimitsUsage;
import BuildCompatibilityInfo = Raven.Server.Web.Studio.UpgradeInfoHandler.BuildCompatibilityInfo;

export class LicenseStubs {
    static licenseServerConnectivityValid() {
        return {
            connected: true,
            exception: null as string,
        };
    }

    static getStatus(): LicenseStatus {
        return {
            Type: "Enterprise",
            Id: "15887ae5-f9c6-4bc8-badf-77ed3d31a42f",
            LicensedTo: "Studio Stubs",
            Status: "Commercial",
            Expired: false,
            FirstServerStartDate: moment()
                .add(-1 as const, "month")
                .format(),
            Ratio: 1,
            Attributes: {
                type: 4,
                version: 50,
                redist: false,
                encryption: true,
                distributedCluster: true,
                maxClusterSize: 1,
                snapshotBackup: true,
                cloudBackup: true,
                dynamicNodesDistribution: true,
                externalReplication: true,
                delayedExternalReplication: true,
                ravenEtl: true,
                sqlEtl: true,
                highlyAvailableTasks: true,
                snmp: true,
                pullReplicationAsHub: true,
                pullReplicationAsSink: true,
                encryptedBackup: true,
                letsEncryptAutoRenewal: true,
                cloud: false,
                documentsCompression: true,
                timeSeriesRollupsAndRetention: true,
                additionalAssembliesNuget: true,
                monitoringEndpoints: true,
                olapEtl: true,
                readOnlyCertificates: true,
                tcpDataCompression: true,
                concurrentSubscriptions: true,
                elasticSearchEtl: true,
                powerBI: true,
                postgreSqlIntegration: true,
                queueEtl: true,
                memory: 2,
                cores: 2,
                expiration: moment()
                    .add(2 as const, "months")
                    .format(),
            },
            FormattedExpiration: null,
            ErrorMessage: null,
            Version: "6.0",
            Expiration: moment()
                .add(2 as const, "months")
                .format(),
            MaxMemory: 2,
            MaxCores: 2,
            IsIsv: false,
            HasEncryption: true,
            HasSnmpMonitoring: true,
            DistributedCluster: true,
            MaxClusterSize: 1,
            HasSnapshotBackups: true,
            HasCloudBackups: true,
            HasDynamicNodesDistribution: true,
            HasExternalReplication: true,
            HasDelayedExternalReplication: true,
            HasRavenEtl: true,
            HasSqlEtl: true,
            HasHighlyAvailableTasks: true,
            HasPullReplicationAsHub: true,
            HasPullReplicationAsSink: true,
            HasEncryptedBackups: true,
            CanAutoRenewLetsEncryptCertificate: true,
            IsCloud: false,
            HasDocumentsCompression: true,
            HasTimeSeriesRollupsAndRetention: true,
            HasAdditionalAssembliesFromNuGet: true,
            HasMonitoringEndpoints: true,
            HasOlapEtl: true,
            HasReadOnlyCertificates: true,
            HasTcpDataCompression: true,
            HasConcurrentDataSubscriptions: true,
            HasElasticSearchEtl: true,
            HasQueueEtl: true,
            HasPowerBI: true,
            HasPostgreSqlIntegration: true,
            HasQueueSink: true,
            CanSetupDefaultRevisionsConfiguration: true,
            HasClientConfiguration: true,
            HasDataArchival: true,
            HasIndexCleanup: true,
            HasMultiNodeSharding: true,
            HasPeriodicBackup: true,
            HasRevisionsInSubscriptions: true,
            HasServerWideBackups: true,
            HasServerWideExternalReplications: true,
            HasServerWideCustomSorters: true,
            HasServerWideAnalyzers: true,
            HasStudioConfiguration: true,
            UpgradeRequired: false,
        };
    }

    static getStatusLimited(): LicenseStatus {
        return {
            ...LicenseStubs.getStatus(),
            Type: "Community",
            MaxNumberOfStaticIndexesPerDatabase: 12,
            MaxNumberOfStaticIndexesPerCluster: 12 * 5,
            MaxNumberOfAutoIndexesPerDatabase: 24,
            MaxNumberOfAutoIndexesPerCluster: 24 * 5,
            MaxNumberOfSubscriptionsPerDatabase: 3,
            MaxNumberOfSubscriptionsPerCluster: 3 * 5,
            MaxNumberOfCustomSortersPerDatabase: 1,
            MaxNumberOfCustomSortersPerCluster: 5,
            MaxNumberOfCustomAnalyzersPerDatabase: 1,
            MaxNumberOfCustomAnalyzersPerCluster: 5,
            MaxNumberOfRevisionsToKeep: 2,
            MaxNumberOfRevisionAgeToKeepInDays: 45,
            MinPeriodForExpirationInHours: 36,
            MinPeriodForRefreshInHours: 36,
        };
    }

    static limitsUsage(): LicenseLimitsUsage {
        return {
            NumberOfStaticIndexesInCluster: 58,
            NumberOfAutoIndexesInCluster: 20,
            NumberOfCustomSortersInCluster: 4,
            NumberOfAnalyzersInCluster: 4,
            NumberOfSubscriptionsInCluster: 14,
        };
    }

    static changeLog(): Raven.Server.Web.Studio.UpgradeInfoHandler.UpgradeInfoResponse {
        return {
            BuildCompatibilitiesForLatestMajorMinor: [
                LicenseStubs.buildCompatibilityInfo("6.0.100"),
                LicenseStubs.buildCompatibilityInfo("6.0.8"),
                LicenseStubs.buildCompatibilityInfo("6.0.7", false),
            ],
            BuildCompatibilitiesForUserMajorMinor: [
                LicenseStubs.buildCompatibilityInfo("6.0.5"),
                LicenseStubs.buildCompatibilityInfo("6.0.0", false),
            ],
            TotalBuildsForLatestMajorMinor: 3,
            TotalBuildsForUserMajorMinor: 2,
        };
    }

    private static buildCompatibilityInfo(fullVersion = "6.0.100", canChange: boolean = true): BuildCompatibilityInfo {
        return {
            CanDowngradeFollowingUpgrade: canChange,
            CanUpgrade: canChange,
            ChangelogHtml: `<h3>Breaking changes</h3>
<ul>
    <li><code>[Backups]</code> compression algorithm was changes from gzip/deflate to zstd, which might introduce some backward compatibility concerns. </li>
</ul>
<hr />
<h3>Server</h3>
<ul>
    <li><code>[Backups]</code> switched to zstd compression algorithm for all backup types and exports. More info <a href=\\"https://github.com/ravendb/ravendb/discussions/17678\\">here</a></li>
</ul>`,
            ReleasedAt: "2023-10-02T07:36:24.3850897",
            FullVersion: fullVersion,
        };
    }

    static support(): Raven.Server.Commercial.LicenseSupportInfo {
        return {
            Status: "ProfessionalSupport",
            EndsAt: moment()
                .add(2 as const, "months")
                .format() as any,
        };
    }

    static configurationSettings(): Raven.Server.Config.Categories.LicenseConfiguration {
        return {
            License: "THIS IS LICENSE",
            CanActivate: true,
            CanRenew: true,
            CanForceUpdate: true,
            DisableAutoUpdate: false,
            EulaAccepted: true,
            DisableLicenseSupportCheck: false,
            DisableAutoUpdateFromApi: false,
            SkipLeasingErrorsLogging: false,
            LicensePath: null,
            ThrowOnInvalidOrMissingLicense: false
        };
    }

    static latestVersion(): Raven.Server.ServerWide.BackgroundTasks.LatestVersionCheck.VersionInfo {
        return {
            Version: "6.0.5",
            BuildNumber: 60050,
            BuildType: "Stable",
            PublishedAt: "2024-01-19T12:58:07.0000000Z",
            UpdateSeverity: "TODO",
        };
    }
}
