using System;
using System.Collections.Generic;
using System.Globalization;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.Commercial
{
    public class LicenseStatus
    {
        public LicenseStatus()
        {
            Message = "No installed license";
        }

        public bool Error { get; set; }

        public Dictionary<string, object> Attributes { get; set; }

        public string Message { get; set; }

        public string Status => Attributes == null ? "AGPL - Open Source" : "Commercial";

        public string ShortDescription
        {
            get
            {
                if (Attributes == null)
                    return null;

                int? cores = null;
                if (Attributes.TryGetValue("cores", out object coresObject) &&
                    coresObject is int)
                {
                    cores = (int)coresObject;
                }

                int? ram = null;
                if (Attributes.TryGetValue("memory", out object memoryObject) &&
                    memoryObject is int)
                {
                    ram = (int)memoryObject;
                }

                var list = new List<string>();
                if (cores != null)
                    list.Add($"{cores} Cores");
                if (ram != null)
                    list.Add($"{(ram.Value == 0 ? "Unlimited" : $"{ram.Value}GB")} RAM");

                return string.Join(", ", list);
            }
        }

        public string FormattedExpiration
        {
            get
            {
                var expiration = Expiration;
                return expiration?.ToString("d", CultureInfo.CurrentCulture);
                
            }
        }

        public LicenseType Type
        {
            get
            {
                if (Error)
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

                return LicenseType.Free;
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

        public int MaxCores => GetValue<int?>("cores") ?? 3;

        public int MaxMemory => GetValue<int?>("memory") ?? 6;

        public int Ratio => MaxMemory / MaxCores;

        public int MaxClusterSize => GetValue<int?>("maxClusterSize") ?? 3;

        public bool DistributedCluster => GetValue<bool>("distributedCluster");

        public bool HasCloudBackups => GetValue<bool>("cloudBackup");

        public bool HasSnapshotBackups => GetValue<bool>("snapshotBackup");

        public bool HasDynamicNodesDistribution => GetValue<bool>("dynamicNodesDistribution");

        public bool HasEncryption => GetValue<bool>("encryption");

        public bool HasExternalReplication => GetValue<bool>("externalReplication");

        public bool HasRavenEtl => GetValue<bool>("ravenEtl");

        public bool HasSqlEtl => GetValue<bool>("sqlEtl");

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(FirstServerStartDate)] = FirstServerStartDate,
                [nameof(Error)] = Error,
                [nameof(Message)] = Message,
                [nameof(Status)] = Status,
                [nameof(ShortDescription)] = ShortDescription,
                [nameof(FormattedExpiration)] = FormattedExpiration,
                [nameof(Type)] = Type.ToString(),
                [nameof(Ratio)] = Ratio.ToString(),
                [nameof(MaxCores)] = MaxCores,
                [nameof(Attributes)] = TypeConverter.ToBlittableSupportedType(Attributes)
            };
        }
    }
}
