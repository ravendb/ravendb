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
            UpgradeRequired: false,
            FirstServerStartDate: moment()
                .add(-1 as const, "month")
                .format(),
            Ratio: 1,
            Attributes: {
                Type: 4,
                Version: "6.0",
                Redist: false,
                Encryption: true,
                DistributedCluster: true,
                MaxClusterSize: 1,
                SnapshotBackup: true,
                CloudBackup: true,
                DynamicNodesDistribution: true,
                ExternalReplication: true,
                DelayedExternalReplication: true,
                RavenEtl: true,
                SqlEtl: true,
                HighlyAvailableTasks: true,
                Snmp: true,
                PullReplicationHub: true,
                PullReplicationSink: true,
                EncryptedBackup: true,
                LetsEncryptAutoRenewal: true,
                Cloud: false,
                DocumentsCompression: true,
                TimeSeriesRollupsAndRetention: true,
                AdditionalAssembliesNuget: true,
                MonitoringEndpoints: true,
                OlapEtl: true,
                ReadOnlyCertificates: true,
                TcpDataCompression: true,
                ConcurrentSubscriptions: true,
                ElasticSearchEtl: true,
                PowerBI: true,
                PostgreSqlIntegration: true,
                QueueEtl: true,
                ServerWideBackups: true,
                ServerWideExternalReplications: true,
                ServerWideCustomSorters: true,
                ServerWideAnalyzers: true,
                IndexCleanup: true,
                PeriodicBackup: true,
                ClientConfiguration: true,
                StudioConfiguration: true,
                QueueSink: true,
                DataArchival: true,
                RevisionsInSubscriptions: true,
                MultiNodeSharding: true,
                SetupDefaultRevisionsConfiguration: true,
                Memory: 24,
                Cores: 2,
                Expiration: moment()
                    .add(2 as const, "months")
                    .format(),
            },
            FormattedExpiration: null,
            ErrorMessage: null,
            Version: "6.0",
            Expiration: moment()
                .add(2 as const, "months")
                .format(),
            MaxMemory: 24,
            MaxCores: 2,
            MaxCoresPerNode: null,
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
            HasSnowflakeEtl: true,
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
            HasServerWideBackups: true,
            HasServerWideExternalReplications: true,
            HasServerWideCustomSorters: true,
            HasServerWideAnalyzers: true,
            HasIndexCleanup: true,
            HasPeriodicBackup: true,
            HasClientConfiguration: true,
            HasStudioConfiguration: true,
            HasQueueSink: true,
            HasDataArchival: true,
            HasRevisionsInSubscriptions: true,
            HasMultiNodeSharding: true,
            MaxNumberOfRevisionsToKeep: null,
            MaxNumberOfRevisionAgeToKeepInDays: null,
            MinPeriodForExpirationInHours: null,
            MinPeriodForRefreshInHours: null,
            MaxReplicationFactorForSharding: null,
            MaxNumberOfStaticIndexesPerDatabase: null,
            MaxNumberOfStaticIndexesPerCluster: null,
            MaxNumberOfAutoIndexesPerDatabase: null,
            MaxNumberOfAutoIndexesPerCluster: null,
            MaxNumberOfSubscriptionsPerDatabase: null,
            MaxNumberOfSubscriptionsPerCluster: null,
            MaxNumberOfCustomSortersPerDatabase: null,
            MaxNumberOfCustomSortersPerCluster: null,
            MaxNumberOfCustomAnalyzersPerDatabase: null,
            MaxNumberOfCustomAnalyzersPerCluster: null,
            CanSetupDefaultRevisionsConfiguration: true,
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
            SupportType: "None",
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
            ThrowOnInvalidOrMissingLicense: false,
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
