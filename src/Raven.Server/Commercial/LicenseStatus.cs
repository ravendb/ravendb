using System;
using System.Collections.Generic;
using System.Globalization;
using Raven.Client.Properties;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.Commercial
{
    public class LicenseStatus
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

        public bool IsIsv => GetValue<bool>(LicenseAttribute.Redist);

        public bool HasEncryption => GetValue<bool>(LicenseAttribute.Encryption);

        public bool HasSnmpMonitoring => GetValue<bool>(LicenseAttribute.Snmp);

        public bool DistributedCluster => GetValue<bool>(LicenseAttribute.DistributedCluster);

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

        public bool HasSnapshotBackups => GetValue<bool>(LicenseAttribute.SnapshotBackup);

        public bool HasCloudBackups => GetValue<bool>(LicenseAttribute.CloudBackup);

        public bool HasDynamicNodesDistribution => GetValue<bool>(LicenseAttribute.DynamicNodesDistribution);

        public bool HasExternalReplication => GetValue<bool>(LicenseAttribute.ExternalReplication);

        public bool HasDelayedExternalReplication => GetValue<bool>(LicenseAttribute.DelayedExternalReplication);

        public bool HasRavenEtl => GetValue<bool>(LicenseAttribute.RavenEtl);

        public bool HasSqlEtl => GetValue<bool>(LicenseAttribute.SqlEtl);

        public bool HasHighlyAvailableTasks => GetValue<bool>(LicenseAttribute.HighlyAvailableTasks);

        public bool HasPullReplicationAsHub => GetValue<bool>(LicenseAttribute.PullReplicationAsHub);

        public bool HasPullReplicationAsSink => GetValue<bool>(LicenseAttribute.PullReplicationAsSink);

        public bool HasEncryptedBackups => GetValue<bool>(LicenseAttribute.EncryptedBackup);

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

        public bool HasAdditionalAssembliesFromNuGet => GetValue<bool>(LicenseAttribute.AdditionalAssembliesNuget);

        public bool HasMonitoringEndpoints => GetValue<bool>(LicenseAttribute.MonitoringEndpoints);

        public bool HasOlapEtl => GetValue<bool>(LicenseAttribute.OlapEtl);

        public bool HasReadOnlyCertificates => GetValue<bool>(LicenseAttribute.ReadOnlyCertificates);

        public bool HasTcpDataCompression => GetValue<bool>(LicenseAttribute.TcpDataCompression);

        public bool HasConcurrentDataSubscriptions => GetValue<bool>(LicenseAttribute.ConcurrentSubscriptions);

        public bool HasElasticSearchEtl => GetValue<bool>(LicenseAttribute.ElasticSearchEtl);
        
        public bool HasQueueEtl => GetValue<bool>(LicenseAttribute.QueueEtl);

        public bool HasPowerBI => GetValue<bool>(LicenseAttribute.PowerBI);

        public bool HasPostgreSqlIntegration => GetValue<bool>(LicenseAttribute.PostgreSqlIntegration);

        public bool HasServerWideExternalReplications => Enabled(LicenseAttribute.ServerWideExternalReplications);
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
                [nameof(HasPostgreSqlIntegration)] = HasPostgreSqlIntegration
            };
        }
    }
}
