using System;
using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Server.Commercial.WriteUsageMetering
{
    public static class WriteUsageMeteringConstants
    {
        public const string WriteUsageEndpointPath = "/api/v1/quill/write-usage";

        public const string UsageQueryEndpointPath = "/api/v1/quill/usage";
    }

    public sealed class WriteUsageDatabaseSnapshot
    {
        public WriteUsageDatabaseSnapshot(string databaseName, string topologyId, string changeVector)
        {
            DatabaseName = databaseName;
            TopologyId = topologyId;
            ChangeVector = changeVector;
        }

        public string DatabaseName { get; }

        public string TopologyId { get; }

        public string ChangeVector { get; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                ["databaseName"] = DatabaseName,
                ["topologyId"] = TopologyId,
                ["changeVector"] = ChangeVector
            };
        }
    }

    public sealed class WriteUsageSnapshot
    {
        public WriteUsageSnapshot(IReadOnlyList<WriteUsageDatabaseSnapshot> databases)
        {
            Databases = databases;
        }

        public IReadOnlyList<WriteUsageDatabaseSnapshot> Databases { get; }
    }


    public sealed class WriteUsageReport
    {
        public WriteUsageReport(DynamicJsonValue license, DateTime sentAtUtc, IReadOnlyList<WriteUsageDatabaseSnapshot> databases)
        {
            License = license;
            SentAtUtc = sentAtUtc;
            Databases = databases;
        }

        public DynamicJsonValue License { get; }

        public DateTime SentAtUtc { get; }

        public IReadOnlyList<WriteUsageDatabaseSnapshot> Databases { get; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(License)] = License,
                [nameof(SentAtUtc)] = SentAtUtc,
                [nameof(Databases)] = new DynamicJsonArray(Databases.Select(d => d.ToJson()))
            };
        }
    }
}
