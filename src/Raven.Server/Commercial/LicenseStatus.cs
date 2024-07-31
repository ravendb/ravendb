using System;
using System.Collections.Generic;
using System.Globalization;
using Raven.Client.Properties;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.Commercial
{
    public sealed class LicenseStatus
    {
        public Guid? Id { get; set; }

        public string LicensedTo { get; set; }

        public Dictionary<LicenseAttribute, object> Attributes { get; set; }

        public string ErrorMessage { get; set; }

        public string Status => Attributes == null ? "AGPL - Open Source" : "Commercial";

        public DateTime FirstServerStartDate { get; set; }

        private T GetValue<T>(LicenseAttribute attributeName, T agplValue = default)
        {
            if (Attributes == null)
                return agplValue;

            if (Attributes.TryGetValue(attributeName, out object value) == false)
                return default(T);

            if (value is T == false)
                return default(T);

            return (T)value;
        }

        private bool Enabled(LicenseAttribute attribute)
        {
            return GetValue<bool>(attribute);
        }

        public bool CanActivate(out DateTime? canBeActivateUntil)
        {
            canBeActivateUntil = GetValue<DateTime?>(LicenseAttribute.CanBeActivatedUntil);
            return canBeActivateUntil == null || canBeActivateUntil.Value >= DateTime.UtcNow.Date;
        }

        public string FormattedExpiration => Expiration?.ToString("dddd, dd MMMM yyyy");

        public bool Expired
        {
            get
            {
                if (Type == LicenseType.None)
                    return false;

                if (Expiration == null)
                    return true;

                return IsIsv ?
                    Expiration < RavenVersionAttribute.Instance.ReleaseDate :
                    DateTime.Compare(Expiration.Value, DateTime.UtcNow) < 0;
            }
        }

        public double Ratio => Math.Max((double)MaxMemory / MaxCores, 1);

        private int MaxClusterSizeInternal => GetValue<int?>(LicenseAttribute.MaxClusterSize) ?? 1;

        public LicenseType Type
        {
            get
            {
                if (ErrorMessage != null)
                    return LicenseType.Invalid;

                if (Attributes == null)
                    return LicenseType.None;

                if (Attributes != null &&
                    Attributes.TryGetValue(LicenseAttribute.Type, out object type) &&
                    type is int)
                {
                    var typeAsInt = (int)type;
                    if (Enum.IsDefined(typeof(LicenseType), typeAsInt))
                        return (LicenseType)typeAsInt;
                }

                return LicenseType.Community;
            }
        }

        public Version Version
        {
            get
            {
                var version = GetValue<string>(LicenseAttribute.Version);
                if (version != null)
                    return new Version(version);

                var intVersion = GetValue<int?>(LicenseAttribute.Version);
                if (intVersion == null)
                    return null;

                var major = intVersion.Value / 10;
                var minor = intVersion.Value % 10;
                return new Version(major, minor);
            }
        }

        public bool UpgradeRequired
        {
            get
            {
                if (Type != LicenseType.Community)
                    return false;

                if (Version == null)
                    return false;

                if (Version.TryParse(RavenVersionAttribute.Instance.Version, out var currentVersion) == false)
                    return false;

                return Version > currentVersion;
            }
        }

        public DateTime? Expiration => GetValue<DateTime?>(LicenseAttribute.Expiration);

        public int MaxMemory => GetValue<int?>(LicenseAttribute.Memory) ?? 6;

        public int MaxCores => GetValue<int?>(LicenseAttribute.Cores) ?? 3;

        public int? MaxCoresPerNode => GetValue<int?>(LicenseAttribute.MaxCoresPerNode, agplValue: 2);

        public int MaxClusterSize
        {
            get
            {
                var maxClusterSize = MaxClusterSizeInternal;
                switch (maxClusterSize)
                {
                    case 0:
                        return int.MaxValue;
                    default:
                        return maxClusterSize;
                }
            }
        }

        public bool IsIsv => Enabled(LicenseAttribute.Redist);

        public bool HasEncryption => Enabled(LicenseAttribute.Encryption);

        public bool HasSnmpMonitoring => Enabled(LicenseAttribute.Snmp);

        public bool DistributedCluster => Enabled(LicenseAttribute.DistributedCluster);

        public bool HasSnapshotBackups => Enabled(LicenseAttribute.SnapshotBackup);

        public bool HasCloudBackups => Enabled(LicenseAttribute.CloudBackup);

        public bool HasDynamicNodesDistribution => Enabled(LicenseAttribute.DynamicNodesDistribution);

        public bool HasExternalReplication => Enabled(LicenseAttribute.ExternalReplication);

        public bool HasDelayedExternalReplication => Enabled(LicenseAttribute.DelayedExternalReplication);

        public bool HasRavenEtl => Enabled(LicenseAttribute.RavenEtl);

        public bool HasSqlEtl => Enabled(LicenseAttribute.SqlEtl);

        public bool HasHighlyAvailableTasks => Enabled(LicenseAttribute.HighlyAvailableTasks);

        public bool HasPullReplicationAsHub => Enabled(LicenseAttribute.PullReplicationHub);

        public bool HasPullReplicationAsSink => Enabled(LicenseAttribute.PullReplicationSink);

        public bool HasEncryptedBackups => Enabled(LicenseAttribute.EncryptedBackup);

        public bool CanAutoRenewLetsEncryptCertificate
        {
            get
            {
                if (Attributes == null)
                    return false;

                if (Attributes.TryGetValue(LicenseAttribute.LetsEncryptAutoRenewal, out var value) == false)
                    return Type != LicenseType.Developer; // backward compatibility

                if (value is bool == false)
                    return false;

                return (bool)value;
            }
        }

        public bool IsCloud => Enabled(LicenseAttribute.Cloud);

        public bool HasDocumentsCompression => Enabled(LicenseAttribute.DocumentsCompression);

        public bool HasTimeSeriesRollupsAndRetention => Enabled(LicenseAttribute.TimeSeriesRollupsAndRetention);

        public bool HasAdditionalAssembliesFromNuGet => Enabled(LicenseAttribute.AdditionalAssembliesNuget);

        public bool HasMonitoringEndpoints => Enabled(LicenseAttribute.MonitoringEndpoints);

        public bool HasOlapEtl => Enabled(LicenseAttribute.OlapEtl);

        public bool HasReadOnlyCertificates => Enabled(LicenseAttribute.ReadOnlyCertificates);

        public bool HasTcpDataCompression => Enabled(LicenseAttribute.TcpDataCompression);

        public bool HasConcurrentDataSubscriptions => Enabled(LicenseAttribute.ConcurrentSubscriptions);

        public bool HasElasticSearchEtl => Enabled(LicenseAttribute.ElasticSearchEtl);

        public bool HasQueueEtl => Enabled(LicenseAttribute.QueueEtl);
        
        public bool HasSnowflakeEtl => Enabled(LicenseAttribute.SnowflakeEtl);

        public bool HasPowerBI => Enabled(LicenseAttribute.PowerBI);

        public bool HasPostgreSqlIntegration => Enabled(LicenseAttribute.PostgreSqlIntegration);

        public bool HasServerWideBackups => Enabled(LicenseAttribute.ServerWideBackups);

        public bool HasServerWideExternalReplications => Enabled(LicenseAttribute.ServerWideExternalReplications);

        public bool HasServerWideCustomSorters => Enabled(LicenseAttribute.ServerWideCustomSorters);

        public bool HasServerWideAnalyzers => Enabled(LicenseAttribute.ServerWideAnalyzers);

        public bool HasIndexCleanup => Enabled(LicenseAttribute.IndexCleanup);

        public bool HasPeriodicBackup => Enabled(LicenseAttribute.PeriodicBackup);

        public bool HasClientConfiguration => Enabled(LicenseAttribute.ClientConfiguration);

        public bool HasStudioConfiguration => Enabled(LicenseAttribute.StudioConfiguration);

        public bool HasQueueSink => Enabled(LicenseAttribute.QueueSink);

        public bool HasDataArchival => Enabled(LicenseAttribute.DataArchival);

        public bool HasRevisionsInSubscriptions => Enabled(LicenseAttribute.RevisionsInSubscriptions);

        public bool HasMultiNodeSharding => Enabled(LicenseAttribute.MultiNodeSharding);

        public bool CanSetupDefaultRevisionsConfiguration => Enabled(LicenseAttribute.SetupDefaultRevisionsConfiguration);

        public int? MaxNumberOfRevisionsToKeep => GetValue<int?>(LicenseAttribute.MaxNumberOfRevisionsToKeep, agplValue: 2);

        public int? MaxNumberOfRevisionAgeToKeepInDays => GetValue<int?>(LicenseAttribute.MaxNumberOfRevisionAgeToKeepInDays, agplValue: 45);

        public int? MinPeriodForExpirationInHours => GetValue<int?>(LicenseAttribute.MinPeriodForExpirationInHours, agplValue: 36);

        public int? MinPeriodForRefreshInHours => GetValue<int?>(LicenseAttribute.MinPeriodForRefreshInHours, agplValue: 36);

        public int? MaxReplicationFactorForSharding => GetValue<int?>(LicenseAttribute.MaxReplicationFactorForSharding, agplValue: 1);

        public int? MaxNumberOfStaticIndexesPerDatabase => GetValue<int?>(LicenseAttribute.MaxNumberOfStaticIndexesPerDatabase, agplValue: null);

        public int? MaxNumberOfStaticIndexesPerCluster => GetValue<int?>(LicenseAttribute.MaxNumberOfStaticIndexesPerCluster, agplValue: null);

        public int? MaxNumberOfAutoIndexesPerDatabase => GetValue<int?>(LicenseAttribute.MaxNumberOfAutoIndexesPerDatabase, agplValue: null);

        public int? MaxNumberOfAutoIndexesPerCluster => GetValue<int?>(LicenseAttribute.MaxNumberOfAutoIndexesPerCluster, agplValue: null);

        public int? MaxNumberOfSubscriptionsPerDatabase => GetValue<int?>(LicenseAttribute.MaxNumberOfSubscriptionsPerDatabase, agplValue: 3);

        public int? MaxNumberOfSubscriptionsPerCluster => GetValue<int?>(LicenseAttribute.MaxNumberOfSubscriptionsPerCluster, agplValue: 3 * 5);

        public int? MaxNumberOfCustomSortersPerDatabase => GetValue<int?>(LicenseAttribute.MaxNumberOfCustomSortersPerDatabase, agplValue: 1);

        public int? MaxNumberOfCustomSortersPerCluster => GetValue<int?>(LicenseAttribute.MaxNumberOfCustomSortersPerCluster, agplValue: 5);

        public int? MaxNumberOfCustomAnalyzersPerDatabase => GetValue<int?>(LicenseAttribute.MaxNumberOfCustomAnalyzersPerDatabase, agplValue: 1);

        public int? MaxNumberOfCustomAnalyzersPerCluster => GetValue<int?>(LicenseAttribute.MaxNumberOfCustomAnalyzersPerCluster, agplValue: 5);

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Id)] = Id?.ToString(),
                [nameof(LicensedTo)] = LicensedTo,
                [nameof(Status)] = Status,
                [nameof(Expired)] = Expired,
                [nameof(UpgradeRequired)] = UpgradeRequired,
                [nameof(FirstServerStartDate)] = FirstServerStartDate,
                [nameof(Ratio)] = Ratio.ToString(CultureInfo.InvariantCulture),
                [nameof(Attributes)] = TypeConverter.ToBlittableSupportedType(Attributes),
                [nameof(ErrorMessage)] = ErrorMessage,
                [nameof(Type)] = Type.ToString(),
                [nameof(Version)] = Version?.ToString(),
                [nameof(Expiration)] = Expiration,
                [nameof(MaxMemory)] = MaxMemory,
                [nameof(MaxCores)] = MaxCores,
                [nameof(MaxCoresPerNode)] = MaxCoresPerNode,
                [nameof(IsIsv)] = IsIsv,
                [nameof(HasEncryption)] = HasEncryption,
                [nameof(HasSnmpMonitoring)] = HasSnmpMonitoring,
                [nameof(DistributedCluster)] = DistributedCluster,
                [nameof(MaxClusterSize)] = MaxClusterSizeInternal,
                [nameof(HasSnapshotBackups)] = HasSnapshotBackups,
                [nameof(HasCloudBackups)] = HasCloudBackups,
                [nameof(HasDynamicNodesDistribution)] = HasDynamicNodesDistribution,
                [nameof(HasExternalReplication)] = HasExternalReplication,
                [nameof(HasDelayedExternalReplication)] = HasDelayedExternalReplication,
                [nameof(HasRavenEtl)] = HasRavenEtl,
                [nameof(HasSqlEtl)] = HasSqlEtl,
                [nameof(HasHighlyAvailableTasks)] = HasHighlyAvailableTasks,
                [nameof(HasPullReplicationAsHub)] = HasPullReplicationAsHub,
                [nameof(HasPullReplicationAsSink)] = HasPullReplicationAsSink,
                [nameof(HasEncryptedBackups)] = HasEncryptedBackups,
                [nameof(CanAutoRenewLetsEncryptCertificate)] = CanAutoRenewLetsEncryptCertificate,
                [nameof(IsCloud)] = IsCloud,
                [nameof(HasDocumentsCompression)] = HasDocumentsCompression,
                [nameof(HasTimeSeriesRollupsAndRetention)] = HasTimeSeriesRollupsAndRetention,
                [nameof(HasAdditionalAssembliesFromNuGet)] = HasAdditionalAssembliesFromNuGet,
                [nameof(HasMonitoringEndpoints)] = HasMonitoringEndpoints,
                [nameof(HasOlapEtl)] = HasOlapEtl,
                [nameof(HasReadOnlyCertificates)] = HasReadOnlyCertificates,
                [nameof(HasTcpDataCompression)] = HasTcpDataCompression,
                [nameof(HasConcurrentDataSubscriptions)] = HasConcurrentDataSubscriptions,
                [nameof(HasElasticSearchEtl)] = HasElasticSearchEtl,
                [nameof(HasQueueEtl)] = HasQueueEtl,
                [nameof(HasSnowflakeEtl)] = HasSnowflakeEtl,
                [nameof(HasPowerBI)] = HasPowerBI,
                [nameof(HasPostgreSqlIntegration)] = HasPostgreSqlIntegration,
                [nameof(HasServerWideBackups)] = HasServerWideBackups,
                [nameof(HasServerWideExternalReplications)] = HasServerWideExternalReplications,
                [nameof(HasServerWideCustomSorters)] = HasServerWideCustomSorters,
                [nameof(HasServerWideAnalyzers)] = HasServerWideAnalyzers,
                [nameof(HasIndexCleanup)] = HasIndexCleanup,
                [nameof(HasPeriodicBackup)] = HasPeriodicBackup,
                [nameof(HasClientConfiguration)] = HasClientConfiguration,
                [nameof(HasStudioConfiguration)] = HasStudioConfiguration,
                [nameof(HasQueueSink)] = HasQueueSink,
                [nameof(HasDataArchival)] = HasDataArchival,
                [nameof(HasRevisionsInSubscriptions)] = HasRevisionsInSubscriptions,
                [nameof(HasMultiNodeSharding)] = HasMultiNodeSharding,
                [nameof(MaxNumberOfRevisionsToKeep)] = MaxNumberOfRevisionsToKeep,
                [nameof(MaxNumberOfRevisionAgeToKeepInDays)] = MaxNumberOfRevisionAgeToKeepInDays,
                [nameof(MinPeriodForExpirationInHours)] = MinPeriodForExpirationInHours,
                [nameof(MinPeriodForRefreshInHours)] = MinPeriodForRefreshInHours,
                [nameof(MaxReplicationFactorForSharding)] = MaxReplicationFactorForSharding,
                [nameof(MaxNumberOfStaticIndexesPerDatabase)] = MaxNumberOfStaticIndexesPerDatabase,
                [nameof(MaxNumberOfStaticIndexesPerCluster)] = MaxNumberOfStaticIndexesPerCluster,
                [nameof(MaxNumberOfAutoIndexesPerDatabase)] = MaxNumberOfAutoIndexesPerDatabase,
                [nameof(MaxNumberOfAutoIndexesPerCluster)] = MaxNumberOfAutoIndexesPerCluster,
                [nameof(MaxNumberOfSubscriptionsPerDatabase)] = MaxNumberOfSubscriptionsPerDatabase,
                [nameof(MaxNumberOfSubscriptionsPerCluster)] = MaxNumberOfSubscriptionsPerCluster,
                [nameof(MaxNumberOfCustomSortersPerDatabase)] = MaxNumberOfCustomSortersPerDatabase,
                [nameof(MaxNumberOfCustomSortersPerCluster)] = MaxNumberOfCustomSortersPerCluster,
                [nameof(MaxNumberOfCustomAnalyzersPerDatabase)] = MaxNumberOfCustomAnalyzersPerDatabase,
                [nameof(MaxNumberOfCustomAnalyzersPerCluster)] = MaxNumberOfCustomAnalyzersPerCluster,
                [nameof(CanSetupDefaultRevisionsConfiguration)] = CanSetupDefaultRevisionsConfiguration,
            };
        }
    }
}
