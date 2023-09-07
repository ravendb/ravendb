import moment from "moment";
import LicenseStatus = Raven.Server.Commercial.LicenseStatus;
import LicenseType = Raven.Server.Commercial.LicenseType;
import LicenseLimitsUsage = Raven.Server.Commercial.LicenseLimitsUsage;

export class LicenseStubs {
    static getStatus(licenseType: LicenseType): LicenseStatus {
        return {
            Type: licenseType,
            Id: "15887ae5-f9c6-4bc8-badf-77ed3d31a42f",
            LicensedTo: "Studio Stubs - " + licenseType,
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
            Version: 50,
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
        };
    }

    static enterprise(): LicenseStatus {
        return LicenseStubs.getStatus("Enterprise");
    }

    static community(): LicenseStatus {
        return {
            ...LicenseStubs.getStatus("Community"),
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
        };
    }

    static limitsUsage(): LicenseLimitsUsage {
        return {
            ClusterAutoIndexes: 20,
            ClusterStaticIndexes: 58,
            ClusterSubscriptionTasks: 14,
        };
    }
}
