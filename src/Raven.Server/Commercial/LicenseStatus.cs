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

        private T GetValue<T>(LicenseAttribute attributeName)
        {
            if (Attributes == null)
                return default(T);

            if (Attributes.TryGetValue(attributeName, out object value) == false)
                return default(T);

            if (value is T == false)
                return default(T);

            return (T)value;
        }

        public bool Enabled(LicenseAttribute attribute)
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

        public int Version => GetValue<int?>(LicenseAttribute.Version) ?? -1;

        public DateTime? Expiration => GetValue<DateTime?>(LicenseAttribute.Expiration);

        public int MaxMemory => GetValue<int?>(LicenseAttribute.Memory) ?? 6;

        public int MaxCores => GetValue<int?>(LicenseAttribute.Cores) ?? 3;

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

        public bool HasPullReplicationAsHub => Enabled(LicenseAttribute.PullReplicationAsHub);

        public bool HasPullReplicationAsSink => Enabled(LicenseAttribute.PullReplicationAsSink);

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

        public bool HasPowerBI => Enabled(LicenseAttribute.PowerBI);

        public bool HasPostgreSqlIntegration => Enabled(LicenseAttribute.PostgreSqlIntegration);

        public bool HasQueueSink => Enabled(LicenseAttribute.KafkaRabbitMQSink);

        public bool HasPeriodicBackup => Enabled(LicenseAttribute.PeriodicBackup);

        public bool HasServerWideTasks => Enabled(LicenseAttribute.ServerWideTasks);

        public bool HasStudioConfiguration => Enabled(LicenseAttribute.StudioConfiguration);

        public bool HasClientConfiguration => Enabled(LicenseAttribute.ClientConfiguration);

        public bool HasIndexCleanup => Enabled(LicenseAttribute.IndexCleanup);

        public bool HasDataArchival => Enabled(LicenseAttribute.DataArchival);

        public int? MaxNumberOfStaticIndexesPerDatabase => GetValue<int?>(LicenseAttribute.MaxNumberOfStaticIndexesPerDatabase) ?? 12;

        public int? MaxNumberOfStaticIndexesPerCluster => GetValue<int?>(LicenseAttribute.MaxNumberOfStaticIndexesPerCluster) ?? 12 * 5;

        public int? MaxNumberOfAutoIndexesPerDatabase => GetValue<int?>(LicenseAttribute.MaxNumberOfAutoIndexesPerDatabase) ?? 24;

        public int? MaxNumberOfAutoIndexesPerCluster => GetValue<int?>(LicenseAttribute.MaxNumberOfAutoIndexesPerCluster) ?? 24 * 5;

        public int? MaxNumberOfSubscriptionsPerDatabase => GetValue<int?>(LicenseAttribute.MaxNumberOfSubscriptionsPerDatabase) ?? 3;

        public int? MaxNumberOfSubscriptionsPerCluster => GetValue<int?>(LicenseAttribute.MaxNumberOfSubscriptionsPerCluster) ?? 3 * 5;

        public int? MaxNumberOfExternalReplicationsPerDatabase => GetValue<int?>(LicenseAttribute.MaxNumberOfExternalReplicationsPerDatabase);

        public int? MaxNumberOfExternalReplicationsPerCluster => GetValue<int?>(LicenseAttribute.MaxNumberOfExternalReplicationsPerCluster);

        public int? MaxNumberOfCustomSortersPerDatabase => GetValue<int?>(LicenseAttribute.MaxNumberOfCustomSortersPerDatabase) ?? 1;

        public int? MaxNumberOfCustomSortersPerCluster => GetValue<int?>(LicenseAttribute.MaxNumberOfCustomSortersPerCluster) ?? 5;

        public int? MaxNumberOfCustomAnalyzersPerDatabase => GetValue<int?>(LicenseAttribute.MaxNumberOfCustomAnalyzersPerDatabase) ?? 1;

        public int? MaxNumberOfCustomAnalyzersPerCluster => GetValue<int?>(LicenseAttribute.MaxNumberOfCustomAnalyzersPerCluster) ?? 5;

        public int? MaxNumberOfRevisionsToKeep => Type == LicenseType.Community ? 2 : null;

        public int? MaxNumberOfRevisionsByAgeToKeep => Type == LicenseType.Community ? 3888000 : null;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Id)] = Id?.ToString(),
                [nameof(LicensedTo)] = LicensedTo,
                [nameof(Status)] = Status,
                [nameof(Expired)] = Expired,
                [nameof(FirstServerStartDate)] = FirstServerStartDate,
                [nameof(Ratio)] = Ratio.ToString(CultureInfo.InvariantCulture),
                [nameof(Attributes)] = TypeConverter.ToBlittableSupportedType(Attributes),
                [nameof(ErrorMessage)] = ErrorMessage,

                [nameof(Type)] = Type.ToString(),
                [nameof(Version)] = Version.ToString(),
                [nameof(Expiration)] = Expiration,
                [nameof(MaxMemory)] = MaxMemory,
                [nameof(MaxCores)] = MaxCores,
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
                [nameof(HasPowerBI)] = HasPowerBI,
                [nameof(HasPostgreSqlIntegration)] = HasPostgreSqlIntegration,
                [nameof(HasQueueSink)] = HasQueueSink,
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
                [nameof(MaxNumberOfRevisionsToKeep)] = MaxNumberOfRevisionsToKeep,
                [nameof(MaxNumberOfRevisionsByAgeToKeep)] = MaxNumberOfRevisionsByAgeToKeep
            };
        }
    }
}
