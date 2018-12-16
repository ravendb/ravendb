using System;
using System.Collections.Generic;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.Commercial
{
    public class LicenseStatus
    {
        public Guid? Id { get; set; }

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

                return DateTime.Compare(Expiration.Value, DateTime.UtcNow) < 0;
            }
        }

        public int MaxCores => GetValue<int?>("cores") ?? 3;

        public int MaxMemory => GetValue<int?>("memory") ?? 6;

        public int Ratio => MaxMemory / MaxCores;

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

        public bool HasCloudBackups => GetValue<bool>("cloudBackup");

        public bool HasSnapshotBackups => GetValue<bool>("snapshotBackup");

        public bool HasDynamicNodesDistribution => GetValue<bool>("dynamicNodesDistribution");

        public bool HasEncryption => GetValue<bool>("encryption");

        public bool HasExternalReplication => GetValue<bool>("externalReplication");

        public bool HasDelayedExternalReplication => GetValue<bool>("delayedExternalReplication");

        public bool HasRavenEtl => GetValue<bool>("ravenEtl");

        public bool HasSqlEtl => GetValue<bool>("sqlEtl");

        public bool HasSnmpMonitoring => GetValue<bool>("snmp");

        public bool HasHighlyAvailableTasks => GetValue<bool>("highlyAvailableTasks");

        public bool HasPullReplication => GetValue<bool>("pullReplication");

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
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
                [nameof(Id)] = Id?.ToString(),
                [nameof(Attributes)] = TypeConverter.ToBlittableSupportedType(Attributes),
                [nameof(HasDynamicNodesDistribution)] = HasDynamicNodesDistribution,
                [nameof(HasEncryption)] = HasEncryption,
                [nameof(HasSnapshotBackups)] = HasSnapshotBackups,
                [nameof(HasCloudBackups)] = HasCloudBackups,
                [nameof(HasExternalReplication)] = HasExternalReplication,
                [nameof(HasDelayedExternalReplication)] = HasDelayedExternalReplication,
                [nameof(HasRavenEtl)] = HasRavenEtl,
                [nameof(HasSqlEtl)] = HasSqlEtl,
                [nameof(HasSnmpMonitoring)] = HasSnmpMonitoring,
                [nameof(DistributedCluster)] = DistributedCluster,
                [nameof(HasHighlyAvailableTasks)] = HasHighlyAvailableTasks,
                [nameof(HasPullReplication)] = HasPullReplication
            };
        }
    }
}
