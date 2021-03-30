using System;
using System.Collections.Generic;
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

        public DateTime? Expiration => GetValue<DateTime?>("expiration");

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

        public int MaxCores => GetValue<int?>("cores") ?? 3;

        public int MaxMemory => GetValue<int?>("memory") ?? 6;

        public double Ratio => Math.Max((double)MaxMemory / MaxCores, 1);

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

        private int MaxClusterSizeInternal => GetValue<int?>("maxClusterSize") ?? 1;

        public bool DistributedCluster => GetValue<bool>("distributedCluster");

        public bool HasSnapshotBackups => GetValue<bool>("snapshotBackup");

        public bool HasCloudBackups => GetValue<bool>("cloudBackup");

        public bool HasEncryptedBackups => GetValue<bool>("encryptedBackup");

        public bool HasDynamicNodesDistribution => GetValue<bool>("dynamicNodesDistribution");

        public bool HasEncryption => GetValue<bool>("encryption");

        public bool HasDocumentsCompression => GetValue<bool>("documentsCompression");

        public bool HasExternalReplication => GetValue<bool>("externalReplication");

        public bool HasDelayedExternalReplication => GetValue<bool>("delayedExternalReplication");

        public bool HasRavenEtl => GetValue<bool>("ravenEtl");

        public bool HasSqlEtl => GetValue<bool>("sqlEtl");
        
        public bool HasOlapEtl => GetValue<bool>("olapEtl");

        public bool HasSnmpMonitoring => GetValue<bool>("snmp");
        
        public bool HasMonitoringEndpoints => GetValue<bool>("monitoringEndpoints");

        public bool HasReadOnlyCertificates => GetValue<bool>("readOnlyCertificates");

        public bool HasHighlyAvailableTasks => GetValue<bool>("highlyAvailableTasks");

        public bool HasPullReplicationAsHub => GetValue<bool>("pullReplicationAsHub");

        public bool HasPullReplicationAsSink => GetValue<bool>("pullReplicationAsSink");

        public bool HasTimeSeriesRollupsAndRetention => GetValue<bool>("timeSeriesRollupsAndRetention");

        public bool HasAdditionalAssembliesFromNuGet => GetValue<bool>("additionalAssembliesNuget");

        public bool IsIsv => GetValue<bool>("redist");

        public bool IsCloud => GetValue<bool>("cloud");

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

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Id)] = Id?.ToString(),
                [nameof(LicensedTo)] = LicensedTo,
                [nameof(Attributes)] = TypeConverter.ToBlittableSupportedType(Attributes),
                [nameof(FirstServerStartDate)] = FirstServerStartDate,
                [nameof(ErrorMessage)] = ErrorMessage,
                [nameof(MaxCores)] = MaxCores,
                [nameof(MaxMemory)] = MaxMemory,
                [nameof(MaxClusterSize)] = MaxClusterSizeInternal,
                [nameof(Ratio)] = Ratio.ToString(),
                [nameof(Expiration)] = Expiration,
                [nameof(Expired)] = Expired,
                [nameof(Status)] = Status,
                [nameof(Type)] = Type.ToString(),
                [nameof(HasDynamicNodesDistribution)] = HasDynamicNodesDistribution,
                [nameof(HasEncryption)] = HasEncryption,
                [nameof(HasDocumentsCompression)] = HasDocumentsCompression,
                [nameof(HasSnapshotBackups)] = HasSnapshotBackups,
                [nameof(HasCloudBackups)] = HasCloudBackups,
                [nameof(HasEncryptedBackups)] = HasEncryptedBackups,
                [nameof(HasExternalReplication)] = HasExternalReplication,
                [nameof(HasDelayedExternalReplication)] = HasDelayedExternalReplication,
                [nameof(HasRavenEtl)] = HasRavenEtl,
                [nameof(HasSqlEtl)] = HasSqlEtl,
                [nameof(HasOlapEtl)] = HasOlapEtl,
                [nameof(HasSnmpMonitoring)] = HasSnmpMonitoring,
                [nameof(HasMonitoringEndpoints)] = HasMonitoringEndpoints,
                [nameof(HasReadOnlyCertificates)] = HasReadOnlyCertificates,
                [nameof(DistributedCluster)] = DistributedCluster,
                [nameof(HasHighlyAvailableTasks)] = HasHighlyAvailableTasks,
                [nameof(HasPullReplicationAsHub)] = HasPullReplicationAsHub,
                [nameof(HasPullReplicationAsSink)] = HasPullReplicationAsSink,
                [nameof(HasTimeSeriesRollupsAndRetention)] = HasTimeSeriesRollupsAndRetention,
                [nameof(HasAdditionalAssembliesFromNuGet)] = HasAdditionalAssembliesFromNuGet,
                [nameof(IsIsv)] = IsIsv,
                [nameof(IsCloud)] = IsCloud,
                [nameof(CanAutoRenewLetsEncryptCertificate)] = CanAutoRenewLetsEncryptCertificate
            };
        }
    }
}
