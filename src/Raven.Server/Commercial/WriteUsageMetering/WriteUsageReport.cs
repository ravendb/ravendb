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
        public WriteUsageDatabaseSnapshot(string topologyId, string applicationName, string changeVector)
        {
            ApplicationName = applicationName;
            TopologyId = topologyId;
            ChangeVector = changeVector;
        }

        public string ApplicationName { get; }

        public string TopologyId { get; }

        public string ChangeVector { get; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(ApplicationName)] = ApplicationName,
                [nameof(TopologyId)] = TopologyId,
                [nameof(ChangeVector)] = ChangeVector
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
        public WriteUsageReport(DynamicJsonValue license, IReadOnlyList<WriteUsageDatabaseSnapshot> applications)
        {
            License = license;
            Applications = applications;
        }

        public DynamicJsonValue License { get; }

        public IReadOnlyList<WriteUsageDatabaseSnapshot> Applications { get; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(License)] = License,
                [nameof(Applications)] = new DynamicJsonArray(Applications.Select(d => d.ToJson()))
            };
        }
    }
}
