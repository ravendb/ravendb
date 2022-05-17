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

        public Dictionary<string, object> Attributes { get; set; }

        public string ErrorMessage { get; set; }

        public string Status => Attributes == null ? "AGPL - Open Source" : "Commercial";

        public DateTime FirstServerStartDate { get; set; }

        private T GetValue<T>(string attributeName)
        {
            if (Attributes == null)
                return default(T);

            if (Attributes.TryGetValue(attributeName, out object value) == false)
                return default(T);

            if (value is T == false)
                return default(T);

            return (T)value;
        }

        public bool CanActivate(out DateTime? canBeActivateUntil)
        {
            canBeActivateUntil = GetValue<DateTime?>("canBeActivatedUntil");
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

        private int MaxClusterSizeInternal => GetValue<int?>("maxClusterSize") ?? 1;

        public LicenseType Type
        {
            get
            {
                if (ErrorMessage != null)
                    return LicenseType.Invalid;

                if (Attributes == null)
                    return LicenseType.None;

                if (Attributes != null &&
                    Attributes.TryGetValue("type", out object type) &&
                    type is int)
                {
                    var typeAsInt = (int)type;
                    if (Enum.IsDefined(typeof(LicenseType), typeAsInt))
                        return (LicenseType)typeAsInt;
                }

                return LicenseType.Community;
            }
        }

        public int Version => GetValue<int?>("version") ?? -1;

        public DateTime? Expiration => GetValue<DateTime?>("expiration");

        public int MaxMemory => GetValue<int?>("memory") ?? 6;

        public int MaxCores => GetValue<int?>("cores") ?? 3;

        public bool IsIsv => GetValue<bool>("redist");

        public bool HasEncryption => GetValue<bool>("encryption");

        public bool HasSnmpMonitoring => GetValue<bool>("snmp");

        public bool DistributedCluster => GetValue<bool>("distributedCluster");

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

        public bool HasSnapshotBackups => GetValue<bool>("snapshotBackup");

        public bool HasCloudBackups => GetValue<bool>("cloudBackup");

        public bool HasDynamicNodesDistribution => GetValue<bool>("dynamicNodesDistribution");

        public bool HasExternalReplication => GetValue<bool>("externalReplication");

        public bool HasDelayedExternalReplication => GetValue<bool>("delayedExternalReplication");

        public bool HasRavenEtl => GetValue<bool>("ravenEtl");

        public bool HasSqlEtl => GetValue<bool>("sqlEtl");

        public bool HasHighlyAvailableTasks => GetValue<bool>("highlyAvailableTasks");

        public bool HasPullReplicationAsHub => GetValue<bool>("pullReplicationAsHub");

        public bool HasPullReplicationAsSink => GetValue<bool>("pullReplicationAsSink");

        public bool HasEncryptedBackups => GetValue<bool>("encryptedBackup");

        public bool CanAutoRenewLetsEncryptCertificate
        {
            get
            {
                if (Attributes == null)
                    return false;

                if (Attributes.TryGetValue("letsEncryptAutoRenewal", out var value) == false)
                    return Type != LicenseType.Developer; // backward compatibility

                if (value is bool == false)
                    return false;

                return (bool)value;
            }
        }

        public bool IsCloud => GetValue<bool>("cloud");

        public bool HasDocumentsCompression => GetValue<bool>("documentsCompression");

        public bool HasTimeSeriesRollupsAndRetention => GetValue<bool>("timeSeriesRollupsAndRetention");

        public bool HasAdditionalAssembliesFromNuGet => GetValue<bool>("additionalAssembliesNuget");

        public bool HasMonitoringEndpoints => GetValue<bool>("monitoringEndpoints");

        public bool HasOlapEtl => GetValue<bool>("olapEtl");

        public bool HasReadOnlyCertificates => GetValue<bool>("readOnlyCertificates");

        public bool HasTcpDataCompression => GetValue<bool>("tcpDataCompression");

        public bool HasConcurrentDataSubscriptions => GetValue<bool>("concurrentSubscriptions");

        public bool HasElasticSearchEtl => GetValue<bool>("elasticSearchEtl");
        
        public bool HasQueueEtl => GetValue<bool>("queueEtl");

        public bool HasPowerBI => GetValue<bool>("powerBI");

        public bool HasPostgreSqlIntegration => GetValue<bool>("postgreSqlIntegration");

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
